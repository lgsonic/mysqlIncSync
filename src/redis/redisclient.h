#ifndef _D_REDISCLIENT_H__
#define _D_REDISCLIENT_H__

#include "hiredis.h"
#include <string>
#include <vector>

typedef long long int64;
typedef std::string string_t;
typedef std::vector<unsigned char> dataBuffer_t;

class CRedisClient
{
public:
	CRedisClient(): m_nTimeout(100), m_redis(NULL) {}
	~CRedisClient()
	{
		__Disconnect();
	}
	void Init(const char *ip, int port);
public:
	bool Exists(const char * szKey, size_t nKeyLen, int64 & nResult);
	bool Get(const char * szKey, size_t nKeyLen, dataBuffer_t & bufResult);
	bool Set(const char * szKey, size_t nKeyLen, const char * pData, size_t nDataLen, size_t Expiry = 0);
	bool Del(const char * szKey, size_t nKeyLen);
	bool IncrBy(const char * szKey, size_t nKeyLen, int64 nIncrement, int64 & nResult);
	bool MGet(const std::vector<string_t> listKey, std::vector<string_t> & listResult);
	bool ZAdd(const char * szKey, size_t nKeyLen, int64 nScore, const char * szMember, size_t nMemberLen);
	bool ZRem(const char * szKey, size_t nKeyLen, const char * szMember, size_t nMemberLen);
	bool ZCard(const char * szKey, size_t nKeyLen, int64 & nSetLen);
	bool ZRange(const char * szKey, size_t nKeyLen, size_t nStart, size_t nEnd, std::vector<int> & listResult);
	bool ZRevRange(const char * szKey, size_t nKeyLen, size_t nStart, size_t nEnd, std::vector<int> & listResult);
	bool ZRemRangeByScore(const char * szKey, size_t nKeyLen, size_t nMin, size_t nMax);
private:
	bool __Reconnect();
	void __Disconnect();
	bool __MakesureConnected();
private:
	string_t m_ServerIp;
	int m_ServerPort;
	size_t m_nTimeout;
	redisContext * m_redis;
};

class CRedisClientPool
{
	typedef CRedisClient * RedisClientPtr_t;
	typedef std::vector<RedisClientPtr_t> RedisClientList_t;
public:
	static CRedisClientPool & GetInstance()
	{
		static CRedisClientPool _s;
		return _s;
	}
	void Init(const char * szIpPorts);
	RedisClientPtr_t GetRedisClient(const char * szKey);
protected:
	CRedisClientPool() {}
	~CRedisClientPool();
private:
	RedisClientList_t m_RedisClientList;
};

class CRedisManager
{
public:
	static CRedisManager & GetInstance()
	{
		static CRedisManager _s;
		return _s;
	}
	void Init(const char * szIpPorts);
	bool Set(const char * pData, size_t nDataLen);
protected:
	CRedisManager() {}
	~CRedisManager() {}
};

#endif
