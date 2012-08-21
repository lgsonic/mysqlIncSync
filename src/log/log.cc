#include "log.h"

#define MAX_PATH	255
#define BUF_SIZE		4096

CLog::LogLevel_t CLog::m_GlobalLevel = CLog::DEBUG;
std::string CLog::m_LogFilePrefix;
std::string CLog::m_LogFilePath;
int CLog::m_LogFileFd = 0;

void CLog::Log(const char * format, ...)
{
	if (m_LogLevel < m_GlobalLevel)
	{
		return ;
	}

	va_list args;
	va_start(args, format);    
	char szBuf[BUF_SIZE] = {0};    
	vsnprintf(szBuf, sizeof(szBuf), format, args);
	va_end(args);
	__WriteLog(szBuf);
}

void CLog::Init(LogLevel_t level)
{
	m_GlobalLevel = level;

	FILE * f = fopen("/proc/self/cmdline", "r");
	if (f)
	{
		char name[MAX_PATH] = {0};
		m_LogFilePrefix = fgets(name, MAX_PATH, f);
		fclose(f);
	}
}

void CLog::__WriteLog(const char * szMsg)
{
	std::string strLevel;
	switch (m_LogLevel)
	{
	case DEBUG:
		{
			strLevel = "[Debug] ";
			break;
		}
	case INFO:
		{
			strLevel = "[Info] ";
			break;
		}
	case WARNING:
		{
			strLevel = "[Warning] ";
			break;
		}
	case ERROR:
		{
			strLevel = "[Error] ";
			break;
		}
	case FATAL:
		{
			strLevel = "[Fatal] ";
			break;
		}
	default:
		{
			strLevel = "[Unknown] ";
			break;
		}
	}

	time_t t = time(0);
       std::ostringstream os;
	os<<"["<<GetDateString(t)<<" "<<GetTimeString(t)<<"] ";
	os<<strLevel<<szMsg<<"\r\n";

	__ResetLogFile();
	if (!write(m_LogFileFd, os.str().c_str(), os.str().size())) {}

}

void CLog::__ResetLogFile()
{
	time_t t = time(0);
	char szCurLogFilePath[MAX_PATH] = {0};
	snprintf(szCurLogFilePath, MAX_PATH, "%s%s.log", m_LogFilePrefix.c_str(), GetDateString(t).c_str());

	if (m_LogFilePath != (std::string)szCurLogFilePath)
	{
		if(m_LogFileFd > 0)
		{
			close(m_LogFileFd);
		}

		m_LogFilePath = szCurLogFilePath;
		m_LogFileFd = open(m_LogFilePath.c_str(), O_RDWR |O_CREAT |O_APPEND , 0755);
	}
}

