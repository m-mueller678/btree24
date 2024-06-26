/*

Copyright (c) 2018 Viktor Leis

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

 */

#pragma once

#if defined(__linux__)

#include <chrono>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include <asm/unistd.h>
#include <linux/perf_event.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include "DataStructureWrapper.hpp"
#include "Tag.hpp"

struct BTreeCppPerfEvent {
    struct event {
        struct read_format {
            uint64_t value;
            uint64_t time_enabled;
            uint64_t time_running;
            uint64_t id;
        };

        perf_event_attr pe;
        int fd;
        read_format prev;
        read_format data;

        double readCounter() {
            double multiplexingCorrection = static_cast<double>(data.time_enabled - prev.time_enabled) /
                                            (data.time_running - prev.time_running);
            return (data.value - prev.value) * multiplexingCorrection;
        }
    };

    std::vector<event> events;
    std::vector<std::string> names;
    std::chrono::time_point<std::chrono::steady_clock> startTime = std::chrono::time_point<std::chrono::steady_clock>::min();
    std::chrono::time_point<std::chrono::steady_clock> stopTime = std::chrono::time_point<std::chrono::steady_clock>::min();
    std::map<std::string, std::string> params;
    bool printHeader;

    BTreeCppPerfEvent() : printHeader(true) {
        registerCounter("cycle", PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES);
        registerCounter("stall_b", PERF_TYPE_HARDWARE, PERF_COUNT_HW_STALLED_CYCLES_BACKEND);
        registerCounter("stall_f", PERF_TYPE_HARDWARE, PERF_COUNT_HW_STALLED_CYCLES_FRONTEND);
        registerCounter("kcycle", PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES, true);
        // registerCounter("scycle", PERF_TYPE_RAW, 0x43FFAE);  // zen3
        registerCounter("instr", PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS);
        // registerCounter("L1-miss", PERF_TYPE_HW_CACHE,
        // PERF_COUNT_HW_CACHE_L1D|(PERF_COUNT_HW_CACHE_OP_READ<<8)|(PERF_COUNT_HW_CACHE_RESULT_MISS<<16));
        registerCounter("L1_miss", PERF_TYPE_RAW, 0x43FF44);  // zen4
        registerCounter("LLC_miss", PERF_TYPE_HARDWARE, PERF_COUNT_HW_CACHE_MISSES);
        registerCounter("br_miss", PERF_TYPE_HARDWARE, PERF_COUNT_HW_BRANCH_MISSES);
        registerCounter("task", PERF_TYPE_SOFTWARE, PERF_COUNT_SW_TASK_CLOCK);

        // additional counters can be found in linux/perf_event.h

        for (unsigned i = 0; i < events.size(); i++) {
            auto &event = events[i];
            event.fd = syscall(__NR_perf_event_open, &event.pe, 0, -1, -1, 0);
            if (event.fd < 0) {
                std::cerr << "Error opening counter " << names[i] << std::endl;
                events.resize(0);
                names.resize(0);
                return;
            }
        }
    }

    void registerCounter(const std::string &name, uint64_t type, uint64_t eventID, bool exclude_user = false) {
        names.push_back(name);
        events.push_back(event());
        auto &event = events.back();
        auto &pe = event.pe;
        memset(&pe, 0, sizeof(struct perf_event_attr));
        pe.type = type;
        pe.size = sizeof(struct perf_event_attr);
        pe.config = eventID;
        pe.disabled = true;
        pe.inherit = 1;
        pe.inherit_stat = 0;
        pe.exclude_user = exclude_user;
        pe.exclude_kernel = false;
        pe.exclude_hv = false;
        pe.read_format = PERF_FORMAT_TOTAL_TIME_ENABLED | PERF_FORMAT_TOTAL_TIME_RUNNING;
    }

    void startCounters() {
        for (unsigned i = 0; i < events.size(); i++) {
            auto &event = events[i];
            ioctl(event.fd, PERF_EVENT_IOC_RESET, 0);
            if (read(event.fd, &event.prev, sizeof(uint64_t) * 3) != sizeof(uint64_t) * 3)
                std::cerr << "Error reading counter " << names[i] << std::endl;
        }
        stopTime = startTime;
        enableCounters();
    }

    void enableCounters() {
        for (unsigned i = 0; i < events.size(); i++) {
            ioctl(events[i].fd, PERF_EVENT_IOC_ENABLE, 0);
        }
        startTime = std::chrono::steady_clock::now() - (stopTime - startTime);
    }

    void disableCounters() {
        for (unsigned i = 0; i < events.size(); i++) {
            ioctl(events[i].fd, PERF_EVENT_IOC_DISABLE, 0);
        }
        stopTime = std::chrono::steady_clock::now();
    }

    ~BTreeCppPerfEvent() {
        for (auto &event: events) {
            close(event.fd);
        }
    }

    void stopCounters() {
        disableCounters();
        for (unsigned i = 0; i < events.size(); i++) {
            auto &event = events[i];
            if (read(event.fd, &event.data, sizeof(uint64_t) * 3) != sizeof(uint64_t) * 3)
                std::cerr << "Error reading counter " << names[i] << std::endl;
        }
    }

    double getDuration() { return std::chrono::duration<double>(stopTime - startTime).count(); }

    double getIPC() { return getCounter("instr") / getCounter("cycle"); }

    double getCPUs() { return getCounter("task") / (getDuration() * 1e9); }

    double getGHz() { return getCounter("cycle") / getCounter("task"); }

    double getCounter(const std::string &name) {
        for (unsigned i = 0; i < events.size(); i++)
            if (names[i] == name)
                return events[i].readCounter();
        return -1;
    }

    static void printCounter(std::ostream &headerOut, std::ostream &dataOut, std::string name, std::string counterValue,
                             bool addComma = true) {
        auto width = std::max(name.length(), counterValue.length());
        headerOut << std::setw(width) << name << (addComma ? "," : "") << " ";
        dataOut << std::setw(width) << counterValue << (addComma ? "," : "") << " ";
    }

    template<typename T>
    static void printCounter(std::ostream &headerOut, std::ostream &dataOut, std::string name, T counterValue,
                             bool addComma = true) {
        std::stringstream stream;
        stream << std::fixed << std::setprecision(3) << counterValue;
        BTreeCppPerfEvent::printCounter(headerOut, dataOut, name, stream.str(), addComma);
    }

    void printReport(std::ostream &out, uint64_t normalizationConstant) {
        std::stringstream header;
        std::stringstream data;
        printReport(header, data, normalizationConstant);
        out << header.str() << std::endl;
        out << data.str() << std::endl;
    }

    void printReport(std::ostream &headerOut, std::ostream &dataOut, uint64_t normalizationConstant) {
        double maybeNan = normalizationConstant == 0 ? 0.0 / 0.0 : 1.0;
        if (!events.size())
            return;

        // print all metrics
        for (unsigned i = 0; i < events.size(); i++) {
            printCounter(headerOut, dataOut, names[i], events[i].readCounter() / normalizationConstant * maybeNan);
        }

        printCounter(headerOut, dataOut, "scale", normalizationConstant * maybeNan);

        // derived metrics
        printCounter(headerOut, dataOut, "IPC", getIPC() * maybeNan);
        printCounter(headerOut, dataOut, "CPU", getCPUs() * maybeNan);
        printCounter(headerOut, dataOut, "GHz", getGHz() * maybeNan, false);
    }

    void setParam(const std::string &name, const std::string &value) { params[name] = value; }

    void setParam(const std::string &name, const char *value) { params[name] = value; }

    template<typename T>
    void setParam(const std::string &name, T value) {
        setParam(name, std::to_string(value));
    }

    void printParams(std::ostream &header, std::ostream &data) {
        for (auto &p: params) {
            printCounter(header, data, p.first, p.second);
        }
    }
};

struct BTreeCppPerfEventBlock {
    BTreeCppPerfEvent &e;
    uint64_t scale;
    std::vector<std::pair<std::string, std::string>> additionalValues;
    DataStructureWrapper *data_strcuture_wrapper;

    BTreeCppPerfEventBlock(BTreeCppPerfEvent &e, DataStructureWrapper &ds, uint64_t scale = 1) : e(e), scale(scale),
                                                                                                 data_strcuture_wrapper(
                                                                                                         &ds) { e.startCounters(); }

    void pushNodeCounts() {
        if (getenv("SKIP_NODE_COUNT"))
            return;
        std::array<uint32_t, TAG_END + 2> counts;
        counts.fill(0);
#if defined(USE_STRUCTURE_BTREE)
        data_strcuture_wrapper->impl.nodeCount(counts);
#endif
        for (int i = TAG_START; i < TAG_END; ++i) {
            push(std::string{"nodeCount_"} + tag_name(Tag(i)), std::to_string(counts[i]));
        }
        push("counted_final_key_count", std::to_string(counts[TAG_END]));
        push("prefix_len_sum", std::to_string(counts[TAG_END + 1]));
    }

    void push(std::string k, std::string v) {
        additionalValues.push_back(std::pair(k, v));
    }

    ~BTreeCppPerfEventBlock() {
        e.stopCounters();
        pushNodeCounts();
        std::stringstream header;
        std::stringstream data;
        e.printParams(header, data);
        BTreeCppPerfEvent::printCounter(header, data, "time", e.getDuration());
        for (auto &x: additionalValues) {
            BTreeCppPerfEvent::printCounter(header, data, x.first, x.second);
        }
        e.printReport(header, data, scale);
        if (e.printHeader) {
            std::cout << header.str() << std::endl;
            e.printHeader = false;
        }
        std::cout << data.str() << std::endl;
    }
};

#else
#include <ostream>
struct BTreeCppPerfEvent {
   void startCounters() {}
   void stopCounters() {}
   void printReport(std::ostream&, uint64_t) {}
};
#endif

#include "config.hpp"

inline BTreeCppPerfEvent makePerfEvent(std::string dataName, unsigned dataSize) {
    BTreeCppPerfEvent e;
    e.setParam("op", "none");
    e.setParam("config_name", configName);
    e.setParam("page_size", pageSize);
    e.setParam("data_name", dataName);
    e.setParam("data_size", dataSize);
    return e;
}