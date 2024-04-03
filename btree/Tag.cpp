#include "Tag.hpp"

const char *tag_name(Tag tag) {
    switch (tag) {
#define T(L)    \
   case Tag::L: \
      return #L;
        T(Leaf)
        T(Inner)
        T(Dense)
        T(Hash)
        T(Dense2)
#undef T
    }
    abort();
}