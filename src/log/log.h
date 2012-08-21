#ifndef _D_LOG_H_
#define _D_LOG_H_

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <unistd.h>
#include <time.h>
#include <fcntl.h>
#include <string>
#include <iostream>
#include <sstream>

#define LOG(x) CLog(CLog::x).Log

inline std::string GetDateString(const time_t & t)
{
	struct tm tmLocal;
	gmtime_r(&t, &tmLocal);
	char szDate[16] = {0};
	snprintf(szDate, 16, "%04d%02d%02d", 1900+tmLocal.tm_year, tmLocal.tm_mon+1, tmLocal.tm_mday);
	return szDate;
}

inline std::string GetTimeString(const time_t & t)
{
	struct tm tmLocal;
	gmtime_r(&t, &tmLocal);
	char szTime[16] = {0};
	snprintf(szTime, 16, "%02d:%02d:%02d", tmLocal.tm_hour, tmLocal.tm_min, tmLocal.tm_sec);
	return szTime;
}

class CLog
{
public:
	enum LogLevel_t {DEBUG,INFO,WARNING,ERROR,FATAL};
public:
	CLog(LogLevel_t level): m_LogLevel(level) {}
	void Log(const char * format, ...);
	static void Init(LogLevel_t level);
	static void SetLogLevel(LogLevel_t level) { m_GlobalLevel = level; }
private:
	void __WriteLog(const char * szMsg);
	static void __ResetLogFile();
private:
	LogLevel_t m_LogLevel;
	
	static LogLevel_t m_GlobalLevel;
	static std::string m_LogFilePrefix;
	static std::string m_LogFilePath;
	static int m_LogFileFd;
};

#endif
