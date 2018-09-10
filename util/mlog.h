#include <iostream>


class RecordSpace {
private:
    int spaces_;
    RecordSpace() {
        spaces_ = 0;
    }
public:
    static RecordSpace *GetInstance(int spaces) {
        static RecordSpace sp;
        sp.spaces_ += spaces;
        for (int i = 0; i < sp.spaces_; i++) {
            std::cout << " ";
        }
        return &sp;
    }
};

#define LOG(space)                              \
    {                                           \
        RecordSpace::GetInstance(space);        \
    }; std::cout
//    std::cout << __FILE__<< ":" << __LINE__ << ":" << __FUNCTION__; \

#define _LOG LOG(0)
#define _BLOG { \
        LOG(0) << __FUNCTION__ << std::endl; LOG(2);\
    }
#define _ELOG LOG(-2)