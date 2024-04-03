#ifndef BTREE24_SEPARATORINFO_HPP
#define BTREE24_SEPARATORINFO_HPP


struct SeparatorInfo {
    unsigned length;   // length of new separator
    unsigned slot;     // slot at which we split
    bool isTruncated;  // if true, we truncate the separator taking length bytes from slot+1
};

#endif //BTREE24_SEPARATORINFO_HPP
