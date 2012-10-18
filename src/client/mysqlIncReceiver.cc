#define MYSQL_CLIENT
#undef MYSQL_SERVER
#include <string>
#include <vector>
#include <fstream>
#include "../iniutil/iniutil.h"
#include "../rapidjson/document.h"
#include "../redis/redisclient.h"
#include "../log/log.h"
#include "../pcre/pcrecpp.h"
#include <my_global.h>

enum Exit_status 
{
  /** No error occurred and execution should continue. */
  OK_CONTINUE= 0,
  /** An error occurred and execution should stop. */
  ERROR_STOP,
  /** No error occurred but execution should stop. */
  OK_STOP
};

std::string to_string(int32 value)
{
  char szBuf[32];
  snprintf(szBuf, 32, "%d", value);
  return szBuf;
}

struct st_RowValue
{
  std::string command;
  std::string table;
  int timestamp;
  int fieldnum;
  std::vector<std::string> new_value_list;
  std::vector<std::string> old_value_list;
  st_RowValue(const std::string & strData)
  {
    timestamp = 0;
    fieldnum = 0;
	
    try
    {
      rapidjson::Document document;
      if (document.Parse<0>(strData.c_str()).HasParseError() == false)
      {
        rapidjson::Value & value = document["command"];
	 if (value.IsString())
        {
          command = value.GetString();
	 }

	 value = document["table"];
	 if (value.IsString())
        {
          table = value.GetString();
	 }

	 value = document["timestamp"];
	 if (value.IsInt())
        {
          timestamp = value.GetInt();
	 }

	 value = document["fieldnum"];
	 if (value.IsInt())
        {
          fieldnum = value.GetInt();
	 }

	 value = document["newrow"];
	 if (value.IsObject())
        {
          for (int i = 0; i < fieldnum; ++i)
          {
            rapidjson::Value & subvalue = value[to_string(i).c_str()];
	     if (subvalue.IsString())
            {
              new_value_list.push_back(subvalue.GetString());
    	     }
          }
	 }

	 value = document["oldrow"];
	 if (value.IsObject())
        {
          for (int i = 0; i < fieldnum; ++i)
          {
            rapidjson::Value & subvalue = value[to_string(i).c_str()];
	     if (subvalue.IsString())
            {
              old_value_list.push_back(subvalue.GetString());
    	     }
          }
	 }
	 
      }
    }
    catch(...) {}
  }
};


struct st_Config
{
  std::string date;
  int64 seq;
  std::string tables;
  std::string redis_servers;
  int redis_dbid;
  int log_level;
};


static const int nMaxPath = 255;
static const int nConfigBufSize = 1024;
static std::string strConfigFile = "mysqlIncReceiver.ini";
static st_Config stConfigValue;


static int get_module_filename(char* name, int size)
{
  FILE * f = fopen("/proc/self/cmdline", "r");
  if (f)
  {
    name = fgets(name, size, f);
    fclose(f);
  }
  
  return strlen(name);
}

static std::string get_pid_filename()
{
  char szPath[nMaxPath] = {0};
  get_module_filename(szPath, nMaxPath);
  return (std::string(szPath)+".pid");
}

static std::string get_app_folder()
{
  char szPath[nMaxPath] = {0};
  get_module_filename(szPath, nMaxPath);
  std::string strPath = szPath;
  int nIndex = strPath.find_last_of("/");
  if (nIndex >= 0)
  {
    return strPath.substr(0, nIndex+1);
  }
  else
  {
    return "";
  }
}

static Exit_status read_config()
{
  std::string strPath = get_app_folder() + strConfigFile;
  CIniReader inireader;
  if (inireader.open(strPath.c_str()))
  {
    return ERROR_STOP;
  }
  
  char szBuf[nConfigBufSize];
  
  int nSize = nConfigBufSize;
  memset(szBuf, 0, nConfigBufSize);
  inireader.getValue("config", "date", szBuf, &nSize);
  stConfigValue.date = szBuf;
  
  nSize = nConfigBufSize;
  memset(szBuf, 0, nConfigBufSize);
  inireader.getValue("config", "seq", szBuf, &nSize);
  stConfigValue.seq = atoll(szBuf);
  
  nSize = nConfigBufSize;
  memset(szBuf, 0, nConfigBufSize);
  inireader.getValue("config", "tables", szBuf, &nSize);
  stConfigValue.tables = szBuf;

  nSize = nConfigBufSize;
  memset(szBuf, 0, nConfigBufSize);
  inireader.getValue("config", "redis_servers", szBuf, &nSize);
  stConfigValue.redis_servers = szBuf;

  nSize = nConfigBufSize;
  memset(szBuf, 0, nConfigBufSize);
  inireader.getValue("config", "redis_dbid", szBuf, &nSize);
  stConfigValue.redis_dbid = atoi(szBuf);
  
  nSize = nConfigBufSize;
  memset(szBuf, 0, nConfigBufSize);
  inireader.getValue("config", "log_level", szBuf, &nSize);
  stConfigValue.log_level = atoi(szBuf);

  inireader.close();
  return OK_CONTINUE;
}

static Exit_status write_config()
{
  std::string strPath = get_app_folder() + strConfigFile;
  CIniWriter iniwriter;
  if (iniwriter.open(strPath.c_str()))
  {
    return ERROR_STOP;
  }
  
  iniwriter.SetValue("config", "date", stConfigValue.date.c_str());
  char szBuf[64] = {0};
  snprintf(szBuf, 64, "%llu", stConfigValue.seq);
  iniwriter.SetValue("config", "seq", szBuf);
  iniwriter.SetValue("config", "tables", stConfigValue.tables.c_str());
  iniwriter.SetValue("config", "redis_servers", stConfigValue.redis_servers.c_str());
  snprintf(szBuf, 64, "%d", stConfigValue.redis_dbid);
  iniwriter.SetValue("config", "redis_dbid", szBuf);
  snprintf(szBuf, 64, "%d", stConfigValue.log_level);
  iniwriter.SetValue("config", "log_level", szBuf);

  iniwriter.close();
  return OK_CONTINUE;
}

static bool check_table_valid(const char * table)
{
  if (!table)
  {
    return false;
  }
  if (stConfigValue.tables.empty())
  {
    return true;
  }

  const char *error;
  int erroffset;
  int rc;
  int ovector[30];
  static pcre *re = pcre_compile(stConfigValue.tables.c_str(), 0, &error, &erroffset, NULL);
  rc = pcre_exec(re, NULL, table, strlen(table), 0, 0, ovector, 30);
  return (rc >= 0);
}


bool display_log(const std::string & strLog)
{
  printf("%s\n", strLog.c_str());

  return true;
}

class Ext_log_process
{
#define REG_LOG_PROCESSOR(x) m_Processors.push_back(&x);
typedef bool (*pFuncProcess)(const std::string & strLog);
public:
  static Ext_log_process & GetInstance()
  {
    static Ext_log_process _s;
    return _s;
  }
  bool Process(const std::string & strLog)
  {
    for (size_t i = 0; i < m_Processors.size(); ++i)
    {
      if (!m_Processors[i](strLog))
      {
        return false;
      }
    }
    return true;
  }
protected:
  Ext_log_process()
  {
    REG_LOG_PROCESSOR(display_log);
  }
protected:
  std::vector<pFuncProcess> m_Processors;
};


void daemon()
{
  int fd;
  
  if (fork() != 0) exit(0); /* parent exits */
  setsid(); /* create a new session */
  if ((fd = open("/dev/null", O_RDWR, 0)) != -1) 
  {
    dup2(fd, STDIN_FILENO);
    dup2(fd, STDOUT_FILENO);
    dup2(fd, STDERR_FILENO);
    if (fd > STDERR_FILENO) close(fd);
  }
  
  /* Try to write the pid file in a best-effort way. */
  FILE *fp = fopen(get_pid_filename().c_str(),"w");
  if (fp) 
  {
    fprintf(fp,"%d\n",(int)getpid());
    fclose(fp);
  }
}


#define MYSQLINCRECEIVER_VERSION "1.0.5"

int main(int argc, char** argv)
{
  if (argc == 2) 
  {
    if (strcmp(argv[1], "-v") == 0)
    {
      printf("version: %s\n", MYSQLINCRECEIVER_VERSION);
    }
    return 0;
  }
  
  daemon();
  
  CLog::Init((CLog::LogLevel_t)0);

  if (read_config() != OK_CONTINUE)
  {
    LOG(ERROR)("read_config failed");
    exit(1);
  }
  
  CLog::SetLogLevel((CLog::LogLevel_t)(stConfigValue.log_level));
  CRedisManager::GetInstance().Init(stConfigValue.redis_servers.c_str(), stConfigValue.redis_dbid);

  string_t strDate;
  int64 nSeq = 0;
  bool bHaveNextDate = false;
  	
  while (1)
  {
    if (!CRedisManager::GetInstance().GetSeq(stConfigValue.date, stConfigValue.seq, strDate, nSeq, bHaveNextDate))
    {
    	usleep(1000000);
	continue;
    }

    if (strDate != stConfigValue.date)
    {
      stConfigValue.seq = 0;
    }

    int64 i = stConfigValue.seq + 1;		
    for ( ; (i <= nSeq) && (i <= stConfigValue.seq + 10000); )
    {
      dataBuffer_t bufResult;
      if (CRedisManager::GetInstance().GetValueBySeq(strDate, i, bufResult))
      {
        std::string strRowValue(bufResult.begin(), bufResult.end());
        st_RowValue row_value(strRowValue);
        if (check_table_valid(row_value.table.c_str()))
        {
          LOG(INFO)("%s.%lld: %s", strDate.c_str(), i, strRowValue.c_str());
          
          if (Ext_log_process::GetInstance().Process(strRowValue))
          {
            ++i;
          }
		  
          continue;
        }
        else
        {
          ++i;
          continue;
        }
      }
      else
      {
        LOG(WARNING)("Get value failed: %s.%lld", strDate.c_str(), i);
	
        int64 nResult = 0;
        if (CRedisManager::GetInstance().ExistsBySeq(strDate, i, nResult))
        {
          if (nResult == 0)
          {
            if (i == nSeq)
            {
              if(bHaveNextDate)							
              {								
                ++i;							
              }							
              else							
              {								
                usleep(10000);							
              }
			  
              break;
            }
            else
            {
              LOG(DEBUG)("Value not exist: %s.%lld", strDate.c_str(), i);
              ++i;
            }
          }
        }
        else
        {
          usleep(1000000);
        }
		
        continue;
      }
    }

    int64 nCurSeq = --i;
    if ((strDate != stConfigValue.date) || (nCurSeq != stConfigValue.seq))
    {
      stConfigValue.date = strDate;
      stConfigValue.seq = nCurSeq;

      static int64 nLasttime = 0;
      int64 nCurtime = time(0);
      if (nCurtime > nLasttime)
      {
        write_config();
        nLasttime = nCurtime;
      }
    }
  }

  return 0;
}


