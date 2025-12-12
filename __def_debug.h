#ifndef EZ_DEF_DEVEL_DEBUG_H
#define EZ_DEF_DEVEL_DEBUG_H

#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifndef LOG_LEVEL_DEBUG
#define LOG_LEVEL_DEBUG 0
#endif

#ifndef LOG_LEVEL_INFO
#define LOG_LEVEL_INFO  1
#endif

#ifndef LOG_LEVEL_WARN
#define LOG_LEVEL_WARN  2
#endif

#ifndef LOG_LEVEL_ERROR
#define LOG_LEVEL_ERROR 3
#endif

#ifndef LOG_LEVEL
#define LOG_LEVEL LOG_LEVEL_INFO
#endif

static inline void ez_get_timestamp(char *buf, size_t buf_size)
{
	struct timespec ts;
	struct tm *tm_info;
	clock_gettime(CLOCK_REALTIME, &ts);
	tm_info = localtime(&ts.tv_sec);
	snprintf(buf, buf_size, "%02d:%02d:%02d.%03ld",
	         tm_info->tm_hour, tm_info->tm_min, tm_info->tm_sec,
	         ts.tv_nsec / 1000000);
}

static inline const char *ez_extract_filename(const char *path)
{
	const char *filename = strrchr(path, '/');
	return filename ? filename + 1 : path;
}

#ifndef LOG_PRINT
#define LOG_PRINT(level_str, ...)                                              \
	do {                                                                       \
		char ts[32];                                                           \
		ez_get_timestamp(ts, sizeof(ts));                                      \
		printf("[%s] [%s] [%s:%d] ", ts, level_str,                            \
		       ez_extract_filename(__FILE__), __LINE__);                       \
		printf(__VA_ARGS__);                                                   \
	} while (0)
#endif

#if LOG_LEVEL <= LOG_LEVEL_DEBUG
#define LOG_DEBUG(...) LOG_PRINT("DEBUG", __VA_ARGS__)
#else
#define LOG_DEBUG(...)
#endif

#if LOG_LEVEL <= LOG_LEVEL_INFO
#define LOG_INFO(...) LOG_PRINT("INFO", __VA_ARGS__)
#else
#define LOG_INFO(...)
#endif

#if LOG_LEVEL <= LOG_LEVEL_WARN
#define LOG_WARN(...) LOG_PRINT("WARN", __VA_ARGS__)
#else
#define LOG_WARN(...)
#endif

#if LOG_LEVEL <= LOG_LEVEL_ERROR
#define LOG_ERROR(...) LOG_PRINT("ERROR", __VA_ARGS__)
#else
#define LOG_ERROR(...)
#endif

#ifndef LOGF
#define LOGF(...) LOG_INFO(__VA_ARGS__)
#endif

#ifndef DATA_LOGF
#define DATA_LOGF(...) LOG_INFO(__VA_ARGS__)
#endif

#ifdef __cplusplus
}
#endif

#endif /* EZ_DEF_DEVEL_DEBUG_H */

