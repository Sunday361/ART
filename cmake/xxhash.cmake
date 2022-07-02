include(FetchContent)

set(XXHASH_GIT_TAG v0.8.0)
set(XXHASH_GIT_URL https://github.com/Cyan4973/xxHash.git)

FetchContent_Declare(
        xxHash
        GIT_REPOSITORY ${XXHASH_GIT_URL}
        GIT_TAG ${XXHASH_GIT_TAG}
)

FetchContent_MakeAvailable(XxHash)