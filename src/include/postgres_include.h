// Wrapper header to include postgres.h with proper extern "C" linkage for C++ compilation
// This file is force-included via CMake -include flag to ensure postgres.h is always first

#ifndef POSTGRES_INCLUDE_H
#define POSTGRES_INCLUDE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <postgres.h>

#ifdef __cplusplus
}
#endif

#endif  // POSTGRES_INCLUDE_H
