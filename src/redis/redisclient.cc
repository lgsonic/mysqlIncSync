#include "redisclient.h"
#include "conhash.h"
#include "../log/log.h"

void CRedisClient::Init(const char *ip, int port, int dbid)
{
	m_ServerIp = ip;
	m_ServerPort = port;
	m_DbId = dbid;

	 __Reconnect();
}

bool CRedisClient::__Reconnect()
{
	__Disconnect();

	struct timeval timeout = { 0, m_nTimeout*1000 };
	m_redis = redisConnectWithTimeout(m_ServerIp.c_str(), m_ServerPort, timeout);
	if(m_redis)
	{
		if(!m_redis->err)
		{
			LOG(INFO)("Redis connect success: %s:%d", m_ServerIp.c_str(), m_ServerPort);
			redisSetTimeout(m_redis, timeout);

			if(SelectDb(m_DbId))
			{
				return true;
			}
			else
			{
				__Disconnect();
				return false;
			}
		}
		else
		{
			LOG(WARNING)("Redis connect failed: %s:%d, errmsg: %s", m_ServerIp.c_str(), m_ServerPort, m_redis->errstr);
			__Disconnect();
			return false;
		}
	}
	else
	{
		LOG(WARNING)("Redis connect failed: %s:%d", m_ServerIp.c_str(), m_ServerPort);
		return false;
	}
}

void CRedisClient::__Disconnect()
{
	if(m_redis)
	{
		redisFree(m_redis);
		m_redis = NULL;
	}
}

bool CRedisClient::__MakesureConnected()
{
	if(m_redis)
	{
		return true;
	}

	return __Reconnect();
}

bool CRedisClient::SelectDb(int nDbId)
{
	if(!__MakesureConnected())
	{
		return false;
	}

	if(nDbId < 0)
	{
		return false;
	}

	bool bRet = false;
	redisReply * reply = (redisReply *)redisCommand(m_redis, "SELECT %d", nDbId);

	if(reply)
	{
		if((reply->type == REDIS_REPLY_ERROR) && reply->str)
		{
			LOG(WARNING)("Redis error: %s", reply->str);
		}
		if(reply->type == REDIS_REPLY_STATUS)
		{
			if((reply->len == 2) && !strncmp(reply->str, "OK", 2))
			{
				bRet = true;
			}
		}

		freeReplyObject(reply);
		reply = NULL;
	}
	else
	{
		LOG(WARNING)("redisCommand failed: SELECT %d", nDbId);
		__Reconnect();
	}

	return bRet;
}

bool CRedisClient::Exists(const char * szKey, size_t nKeyLen, int64 & nResult)
{
	if(!__MakesureConnected())
	{
		return false;
	}

	bool bRet = false;
	redisReply * reply = (redisReply *)redisCommand(m_redis, "EXISTS %b", szKey, nKeyLen);

	if(reply)
	{
		if((reply->type == REDIS_REPLY_ERROR) && reply->str)
		{
			LOG(WARNING)("Redis error: %s", reply->str);
		}
		if(reply->type == REDIS_REPLY_INTEGER)
		{
			bRet = true;
			nResult = reply->integer;
		}

		freeReplyObject(reply);
		reply = NULL;
	}
	else
	{
		LOG(WARNING)("redisCommand failed: EXISTS %s", szKey);
		__Reconnect();
	}
	
	return bRet;
}

bool CRedisClient::Get(const char * szKey, size_t nKeyLen, dataBuffer_t & bufResult)
{
	if(!__MakesureConnected())
	{
		return false;
	}

	bool bRet = false;
	redisReply * reply = (redisReply *)redisCommand(m_redis, "GET %b", szKey, nKeyLen);

	if(reply)
	{
		if((reply->type == REDIS_REPLY_ERROR) && reply->str)
		{
			LOG(WARNING)("Redis error: %s", reply->str);
		}
		if((reply->type == REDIS_REPLY_STRING) && (reply->len > 0) && reply->str)
		{
			bRet = true;
			bufResult.insert(bufResult.end(), reply->str, reply->str + reply->len);
		}

		freeReplyObject(reply);
		reply = NULL;
	}
	else
	{
		LOG(WARNING)("redisCommand failed: GET %s", szKey);
		__Reconnect();
	}

	return bRet;
}

bool CRedisClient::Set(const char * szKey, size_t nKeyLen, const char * pData, size_t nDataLen, size_t Expiry)
{
	if(!__MakesureConnected())
	{
		return false;
	}

	bool bRet = false;
	redisReply * reply;
	if(Expiry > 0)
	{
		reply = (redisReply *)redisCommand(m_redis, "SETEX %b %d %b", szKey, nKeyLen, Expiry, pData, nDataLen);
	}
	else
	{
		reply = (redisReply *)redisCommand(m_redis, "SET %b %b", szKey, nKeyLen, pData, nDataLen);
	}
	
	if(reply)
	{
		if((reply->type == REDIS_REPLY_ERROR) && reply->str)
		{
			LOG(WARNING)("Redis error: %s", reply->str);
		}
		if(reply->type == REDIS_REPLY_STATUS)
		{
			if((reply->len == 2) && !strncmp(reply->str, "OK", 2))
			{
				bRet = true;
			}
		}

		freeReplyObject(reply);
		reply = NULL;
	}
	else
	{
		LOG(WARNING)("redisCommand failed: SET %s", szKey);
		__Reconnect();
	}

	return bRet;
}

bool CRedisClient::Del(const char * szKey, size_t nKeyLen)
{
	if(!__MakesureConnected())
	{
		return false;
	}

	bool bRet = false;
	redisReply * reply = (redisReply *)redisCommand(m_redis, "DEL %b", szKey, nKeyLen);

	if(reply)
	{
		if((reply->type == REDIS_REPLY_ERROR) && reply->str)
		{
			LOG(WARNING)("Redis error: %s", reply->str);
		}
		if(reply->type == REDIS_REPLY_INTEGER)
		{
			bRet = true;
		}
			
		freeReplyObject(reply);
		reply = NULL;
	}
	else
	{
		LOG(WARNING)("redisCommand failed: DEL %s", szKey);
		__Reconnect();
	}

	return bRet;
}

bool CRedisClient::IncrBy(const char * szKey, size_t nKeyLen, int64 nIncrement, int64 & nResult)
{
	if(!__MakesureConnected())
	{
		return false;
	}

	bool bRet = false;
	redisReply * reply = (redisReply *)redisCommand(m_redis, "INCRBY %b %lld", szKey, nKeyLen, nIncrement);

	if(reply)
	{
		if((reply->type == REDIS_REPLY_ERROR) && reply->str)
		{
			LOG(WARNING)("Redis error: %s", reply->str);
		}
		if(reply->type == REDIS_REPLY_INTEGER)
		{
			bRet = true;
			nResult = reply->integer;
		}

		freeReplyObject(reply);
		reply = NULL;
	}
	else
	{
		LOG(WARNING)("redisCommand failed: INCRBY %s %lld", szKey, nIncrement);
		__Reconnect();
	}
	
	return bRet;
}

bool CRedisClient::MGet(const std::vector<string_t> listKey, std::vector<string_t> & listResult)
{
	if(!__MakesureConnected())
	{
		return false;
	}

	if(listKey.empty())
	{
		return false;
	}

	int argc = listKey.size() + 1;
	const char * argv[argc];
	size_t argvlen[argc];
	argv[0] = "MGET";
	argvlen[0] = 4;
	for(int i = 1; i < argc; ++i)
	{
		argv[i] = listKey[i-1].c_str();
		argvlen[i] = listKey[i-1].size();
	}

	bool bRet = false;
	redisReply * reply = (redisReply *)redisCommandArgv(m_redis, argc, argv, argvlen);

	if(reply)
	{
		if((reply->type == REDIS_REPLY_ERROR) && reply->str)
		{
			LOG(WARNING)("Redis error: %s", reply->str);
		}
		if(reply->type == REDIS_REPLY_ARRAY)
		{
			bRet = true;

			if(reply->elements > 0)
			{
				for(size_t i = 0; i < reply->elements; ++i)
				{
					redisReply * listReply = reply->element[i];

					if(listReply && (listReply->type == REDIS_REPLY_STRING) && (listReply->len > 0) && listReply->str)
					{
						listResult.push_back(listReply->str);
					}
					else
					{
						listResult.push_back("");
					}
				}
			}
		}

		freeReplyObject(reply);
		reply = NULL;
	}
	else
	{
		LOG(WARNING)("redisCommand failed: MGET %s ...", listKey[0].c_str());
		
		__Reconnect();
	}
	
	return bRet;
}

bool CRedisClient::ZAdd(const char * szKey, size_t nKeyLen, int64 nScore, const char * szMember, size_t nMemberLen)
{
	if(!__MakesureConnected())
	{
		return false;
	}

	bool bRet = false;
	redisReply * reply = (redisReply *)redisCommand(m_redis, "ZADD %b %lld %b", szKey, nKeyLen, nScore, szMember, nMemberLen);
	
	if(reply)
	{
		if((reply->type == REDIS_REPLY_ERROR) && reply->str)
		{
			LOG(WARNING)("Redis error: %s", reply->str);
		}
		if(reply->type == REDIS_REPLY_INTEGER)
		{
			bRet = true;
		}

		freeReplyObject(reply);
		reply = NULL;
	}
	else
	{
		LOG(WARNING)("redisCommand failed: ZADD %s %lld %s", szKey, nScore, szMember);
		__Reconnect();
	}
	
	return bRet;
}

bool CRedisClient::ZRem(const char * szKey, size_t nKeyLen, const char * szMember, size_t nMemberLen)
{
	if(!__MakesureConnected())
	{
		return false;
	}

	bool bRet = false;
	redisReply * reply = (redisReply *)redisCommand(m_redis, "ZREM %b %b", szKey, nKeyLen, szMember, nMemberLen);
	
	if(reply)
	{
		if((reply->type == REDIS_REPLY_ERROR) && reply->str)
		{
			LOG(WARNING)("Redis error: %s", reply->str);
		}
		if(reply->type == REDIS_REPLY_INTEGER)
		{
			bRet = true;
		}

		freeReplyObject(reply);
		reply = NULL;
	}
	else
	{
		LOG(WARNING)("redisCommand failed: ZREM %s %s", szKey, szMember);
		__Reconnect();
	}
	
	return bRet;
}

bool CRedisClient::ZCard(const char * szKey, size_t nKeyLen, int64 & nSetLen)
{
	if(!__MakesureConnected())
	{
		return false;
	}

	bool bRet = false;
	redisReply * reply = (redisReply *)redisCommand(m_redis, "ZCARD %b", szKey, nKeyLen);
	
	if(reply)
	{
		if((reply->type == REDIS_REPLY_ERROR) && reply->str)
		{
			LOG(WARNING)("Redis error: %s", reply->str);
		}
		if(reply->type == REDIS_REPLY_INTEGER)
		{
			bRet = true;
			nSetLen = reply->integer;
		}

		freeReplyObject(reply);
		reply = NULL;
	}
	else
	{
		LOG(WARNING)("redisCommand failed: ZCARD %s", szKey);
		__Reconnect();
	}

	return bRet;
}

bool CRedisClient::ZRange(const char * szKey, size_t nKeyLen, size_t nStart, size_t nEnd, std::vector<int> & listResult)
{
	if(!__MakesureConnected())
	{
		return false;
	}

	bool bRet = false;
	redisReply * reply = (redisReply *)redisCommand(m_redis, "ZRANGE %b %d %d", szKey, nKeyLen, nStart, nEnd);

	if(reply)
	{
		if((reply->type == REDIS_REPLY_ERROR) && reply->str)
		{
			LOG(WARNING)("Redis error: %s", reply->str);
		}
		if(reply->type == REDIS_REPLY_ARRAY)
		{
			bRet = true;

			if(reply->elements > 0)
			{
				for(size_t i = 0; i < reply->elements; ++i)
				{
					redisReply * listReply = reply->element[i];

					if(listReply && (listReply->type == REDIS_REPLY_STRING) && (listReply->len > 0) && listReply->str)
					{
						listResult.push_back(atol(listReply->str));
					}
				}
			}
		}

		freeReplyObject(reply);
		reply = NULL;
	}
	else
	{
		LOG(WARNING)("redisCommand failed: ZRANGE %s %d %d", szKey, nStart, nEnd);
		__Reconnect();
	}
	
	return bRet;
}

bool CRedisClient::ZRevRange(const char * szKey, size_t nKeyLen, size_t nStart, size_t nEnd, std::vector<int> & listResult)
{
	if(!__MakesureConnected())
	{
		return false;
	}

	bool bRet = false;
	redisReply * reply = (redisReply *)redisCommand(m_redis, "ZREVRANGE %b %d %d", szKey, nKeyLen, nStart, nEnd);

	if(reply)
	{
		if((reply->type == REDIS_REPLY_ERROR) && reply->str)
		{
			LOG(WARNING)("Redis error: %s", reply->str);
		}
		if(reply->type == REDIS_REPLY_ARRAY)
		{
			bRet = true;

			if(reply->elements > 0)
			{
				for(size_t i = 0; i < reply->elements; ++i)
				{
					redisReply * listReply = reply->element[i];

					if(listReply && (listReply->type == REDIS_REPLY_STRING) && (listReply->len > 0) && listReply->str)
					{
						listResult.push_back(atol(listReply->str));
					}
				}
			}
		}

		freeReplyObject(reply);
		reply = NULL;
	}
	else
	{
		LOG(WARNING)("redisCommand failed: ZREVRANGE %s %d %d", szKey, nStart, nEnd);
		__Reconnect();
	}
	
	return bRet;
}

bool CRedisClient::ZRemRangeByScore(const char * szKey, size_t nKeyLen, size_t nMin, size_t nMax)
{
	if(!__MakesureConnected())
	{
		return false;
	}

	bool bRet = false;
	redisReply * reply = (redisReply *)redisCommand(m_redis, "ZREMRANGEBYSCORE %b %d %d", szKey, nKeyLen, nMin, nMax);

	if(reply)
	{
		if((reply->type == REDIS_REPLY_ERROR) && reply->str)
		{
			LOG(WARNING)("Redis error: %s", reply->str);
		}
		if(reply->type == REDIS_REPLY_INTEGER)
		{
			bRet = true;
		}

		freeReplyObject(reply);
		reply = NULL;
	}
	else
	{
		LOG(WARNING)("redisCommand failed: ZREMRANGEBYSCORE %s %d %d", szKey, nMin, nMax);
		__Reconnect();
	}
	
	return bRet;
}

static struct conhash_s * s_RedisConhash;
static struct node_s * s_RedisNodes;

CRedisClientPool::~CRedisClientPool()
{
	for(size_t i = 0; i < m_RedisClientList.size(); ++i)			
	{
		delete m_RedisClientList[i];
	}

	if(s_RedisConhash != NULL)
	{
		conhash_fini(s_RedisConhash);
	}

	if(s_RedisNodes != NULL)
	{
		delete[] s_RedisNodes;
	}
}

void CRedisClientPool::Init(const char * szIpPorts, int nDbId)
{
	if(!szIpPorts)
	{
		return;
	}

	int nRedisNum = 0;
	string_t strIpPorts = szIpPorts;
	int nPos = strIpPorts.find(":");
	while(nPos > 0)
	{
		string_t strIp = strIpPorts.substr(0, nPos);
		int nPort = atoi(strIpPorts.c_str()+nPos+1);
		
		nRedisNum++;
		RedisClientPtr_t pRedisClient = new CRedisClient();
		pRedisClient->Init(strIp.c_str(), nPort, nDbId);
		m_RedisClientList.push_back(pRedisClient);

		int nPos2 = strIpPorts.find(",");
		if(nPos2 > 0)
		{
			strIpPorts = strIpPorts.substr(nPos2+1);
			nPos = strIpPorts.find(":");
		}
		else
		{
			nPos = 0;
		}
	}

	if(!nRedisNum)
	{
		return;
	}

	s_RedisConhash = conhash_init(NULL);		
	if(s_RedisConhash)		
	{			
		s_RedisNodes = new struct node_s[nRedisNum];			
		for(int i = 0; i < nRedisNum; ++i)			
		{
			char szName[16] = {0};
			snprintf(szName, 16, "%d", i);
			conhash_set_node(&s_RedisNodes[i], szName, 10, i);				
			conhash_add_node(s_RedisConhash, &s_RedisNodes[i]);			
		}		
	}
}

CRedisClientPool::RedisClientPtr_t CRedisClientPool::GetRedisClient(const char * szKey)
{
	if(m_RedisClientList.empty())
	{
		return NULL;
	}
	
	const struct node_s * node = conhash_lookup(s_RedisConhash, szKey);			
	if(node && (node->seq < m_RedisClientList.size()))		
	{			
		return m_RedisClientList[node->seq];
	}
	else
	{
		return m_RedisClientList[0];
	}
}

CRedisClientPool::RedisClientPtr_t CRedisClientPool::GetRedisClient(int nSeq)
{
	if((nSeq >= 0) && (nSeq < (int)m_RedisClientList.size()))
	{
		return m_RedisClientList[nSeq];
	}
	else
	{
		return m_RedisClientList[0];
	}
}

void CRedisManager::Init(const char * szIpPorts, int nDbId)
{
	CRedisClientPool::GetInstance().Init(szIpPorts, nDbId);
}

static const string_t strDateKey = "datelist";
static const string_t strSeqKeyPrefix = "s.";
static const string_t strValueKeyPrefix = "v.";
static const size_t nRecentDay = 3;
static const size_t nExpiry = 259200;
static string_t strLastDate;
	
bool CRedisManager::SetValue(const char * pData, size_t nDataLen)
{
	CRedisClient * pRedisClient = NULL;
	time_t t = time(0);
	string_t strDate = GetDateString(t);
	int nDate = atoi(strDate.c_str());
	if(strDate != strLastDate)
	{
		pRedisClient = CRedisClientPool::GetInstance().GetRedisClient(0);
		if(!pRedisClient) return false;
		if(!pRedisClient->ZAdd(strDateKey.c_str(), strDateKey.size(), nDate, strDate.c_str(), strDate.size())) return false;
		std::vector<int> nDateList;
		if(!pRedisClient->ZRevRange(strDateKey.c_str(), strDateKey.size(), 0, nRecentDay-1, nDateList)) return false;
		if(nDateList.size() == nRecentDay)
		{
			pRedisClient->ZRemRangeByScore(strDateKey.c_str(), strDateKey.size(), 0, nDateList[nRecentDay-1]-1);
		}
		
		strLastDate = strDate;
	}
		
	int64 nSeq = 0;
	string_t strSeqKey = strSeqKeyPrefix + strDate;
	pRedisClient = CRedisClientPool::GetInstance().GetRedisClient(0);
	if(!pRedisClient) return false;
	if(!pRedisClient->IncrBy(strSeqKey.c_str(), strSeqKey.size(), 1, nSeq)) return false;
	
	char szKey[32] = {0};
	snprintf(szKey, 32, "%s%s.%lld", strValueKeyPrefix.c_str(), strDate.c_str(), nSeq);
	pRedisClient = CRedisClientPool::GetInstance().GetRedisClient(szKey);
	if(!pRedisClient) return false;
	return pRedisClient->Set(szKey, strlen(szKey), pData, nDataLen, nExpiry);
}

bool CRedisManager::GetValue(const char * szKey, size_t nKeyLen, dataBuffer_t & bufResult)
{
	CRedisClient * pRedisClient = CRedisClientPool::GetInstance().GetRedisClient(szKey);
	if(!pRedisClient) return false;
	return pRedisClient->Get(szKey, nKeyLen, bufResult);
}

bool CRedisManager::GetValueBySeq(const string_t & strDate, const int64 & nSeq , dataBuffer_t & bufResult)
{
	char szKey[32] = {0};
	snprintf(szKey, 32, "%s%s.%lld", strValueKeyPrefix.c_str(), strDate.c_str(), nSeq);
	
	CRedisClient * pRedisClient = CRedisClientPool::GetInstance().GetRedisClient(szKey);
	if(!pRedisClient) return false;
	return pRedisClient->Get(szKey, strlen(szKey), bufResult);
}

bool CRedisManager::ExistsBySeq(const string_t & strDate, const int64 & nSeq, int64 & nResult)
{
	char szKey[32] = {0};
	snprintf(szKey, 32, "%s%s.%lld", strValueKeyPrefix.c_str(), strDate.c_str(), nSeq);

	CRedisClient * pRedisClient = CRedisClientPool::GetInstance().GetRedisClient(szKey);
	if(!pRedisClient) return false;
	return pRedisClient->Exists(szKey, strlen(szKey), nResult);
}

bool CRedisManager::GetSeq(const string_t & strCurDate, const int64 & nCurSeq, string_t & strDate, int64 & nSeq)
{
	CRedisClient * pRedisClient = CRedisClientPool::GetInstance().GetRedisClient(0);
	if(!pRedisClient) return false;
	std::vector<int> nDateList;
	if(!pRedisClient->ZRange(strDateKey.c_str(), strDateKey.size(), 0, nRecentDay-1, nDateList)) return false;

	int nCurDate = atoi(strCurDate.c_str());
	for(size_t i = 0; i < nDateList.size(); ++i)
	{
		if(nDateList[i] == nCurDate)
		{
			strDate = strCurDate;
			if(!__GetSeqByDate(strDate, nSeq)) return false;

			if(nSeq > nCurSeq)
			{
				return true;
			}
			else if((i+1) < nDateList.size())
			{
				char szDate[10] = {0};
				snprintf(szDate, 10, "%d", nDateList[i+1]);
				strDate = szDate;
				if(!__GetSeqByDate(strDate, nSeq)) return false;
				return true;
			}
			else return false;
		}
	}

	if(nDateList.size() > 0)
	{
		char szDate[10] = {0};
		snprintf(szDate, 10, "%d", nDateList[nDateList.size()-1]);
		strDate = szDate;
		return __GetSeqByDate(strDate, nSeq);
	}
	
	return false;
}

bool CRedisManager::__GetSeqByDate(const string_t & strDate, int64 & nSeq)
{
	string_t strSeqKey = strSeqKeyPrefix + strDate;
	dataBuffer_t bufResult;
	CRedisClient * pRedisClient = CRedisClientPool::GetInstance().GetRedisClient(0);
	if(!pRedisClient) return false;
	if(!pRedisClient->Get(strSeqKey.c_str(), strSeqKey.size(), bufResult)) return false;
	string_t strSeqValue(bufResult.begin(), bufResult.end());
	nSeq = atoll(strSeqValue.c_str());
	return true;
}
