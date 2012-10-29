#ifndef _D_INIUTIL_H_
#define _D_INIUTIL_H_

#include <string.h>

#define _snprintf snprintf
#define stricmp strcasecmp
#define strnicmp strncasecmp

enum 
{
	IDD_ERR_ZERO 				= 0,

	IDD_ERR_INVALID_VALUE 		= -100,
	IDD_ERR_VALUE_NAME_OVERSIZE = -101,
	IDD_ERR_VALUE_OVERSIZE 		= -102,
	IDD_ERR_VALUE_NOTFOUND		= -103, 
	IDD_ERR_SECTION_NOTFOUND	= -104,
	IDD_ERR_INIFILE_OPENFAILED  = -105,
	IDD_ERR_PARAM_INVALID		= -106
};

const char *getIDDErrDesc(int err);

enum 
{
	IDD_SECTION_NAME_SIZE = 63,
	IDD_VALUE_NAME_SIZE = 63,
	IDD_VALUE_SIZE = 10240
};

struct IDD_Value 
{
	char name[IDD_VALUE_NAME_SIZE + 1];
	char value[IDD_VALUE_SIZE + 1];
};

struct IDD_Section 
{
	char name[IDD_SECTION_NAME_SIZE + 1];
	std::vector<struct IDD_Value *> valuelist;
};

class CIniReader 
{
public:
	int open(const char *IniPath);
	int close();

	int getValue(const char *SectionName, const char *ValueName, char *ValueBuf, int *BufSize);
	int getValueAt(const char *SectionName, const char *ValueName, char *ValueBuf, int *BufSize, int index);
	int getValueCount(const char *SectionName, const char *ValueName);
	
	void dumpStdout();

private:
	std::vector<struct IDD_Section*> sectionlist;

	void _clearSectionList();
	int _suckValue(char *LineBuf, char *NameBuf, char *ValueBuf);
	int __removeRN(char *LineBuf);
	int __removeEndMark(char *LineBuf, char MarkChar);

	std::vector<struct IDD_Section *>::iterator _iter_section;
	struct IDD_Section *_this_section;
	std::vector<struct IDD_Value *> *_value_listp;
	struct IDD_Value *_this_value;
	std::vector<struct IDD_Value *>::iterator _iter_value;
};

class CIniWriter: public CIniReader
{
public:
	int open(const char *IniPath);
	int close();

	int SetValue(const char *SectionName, const char *ValueName, const char *ValueBuf);
	int SetValueAt(const char *SectionName, const char *ValueName, const char *ValueBuf, int index);
	int getValueCount(const char *SectionName, const char *ValueName);
	
	void dumpStdout();

private:
	int dumpFile();
	const char *szIniPath;

	std::vector<struct IDD_Section*> sectionlist;

	void _clearSectionList();
	int _suckValue(char *LineBuf, char *NameBuf, char *ValueBuf);
	int __removeRN(char *LineBuf);
	int __removeEndMark(char *LineBuf, char MarkChar);

	std::vector<struct IDD_Section *>::iterator _iter_section;
	struct IDD_Section *_this_section;
	std::vector<struct IDD_Value *> *_value_listp;
	struct IDD_Value *_this_value;
	std::vector<struct IDD_Value *>::iterator _iter_value;
};

const char *getIDDErrDesc(int err)
{
	if(err == IDD_ERR_ZERO) return "IDD_ERR_ZERO";
	if(err == IDD_ERR_INVALID_VALUE) return "IDD_ERR_INVALID_VALUE";
	if(err == IDD_ERR_VALUE_NAME_OVERSIZE) return "IDD_ERR_VALUE_NAME_OVERSIZE";
	if(err == IDD_ERR_VALUE_OVERSIZE) return "IDD_ERR_VALUE_OVERSIZE";
	if(err == IDD_ERR_VALUE_NOTFOUND) return "IDD_ERR_VALUE_NOTFOUND";
	if(err == IDD_ERR_SECTION_NOTFOUND) return "IDD_ERR_SECTION_NOTFOUND";
    if(err == IDD_ERR_INIFILE_OPENFAILED) return "IDD_ERR_INIFILE_OPENFAILED";
	return "IDD_ERR_Unknown";
}

int CIniReader::__removeEndMark(char *LineBuf, char MarkChar)
{
	char *_pt;
	for(_pt = LineBuf + strlen(LineBuf) - 1; _pt >= 0; _pt --)
	{
		if((*_pt) == MarkChar) 
		{
			*_pt = '\0';
			break;
		}
		*_pt = '\0';
	}
	return 0;
}

int CIniReader::__removeRN(char *LineBuf)
{
	if(LineBuf[strlen(LineBuf) - 1] == 0x0D || LineBuf[strlen(LineBuf) - 1] == 0x0A) LineBuf[strlen(LineBuf) - 1] = 0x00;
	if(LineBuf[strlen(LineBuf) - 1] == 0x0D || LineBuf[strlen(LineBuf) - 1] == 0x0A) LineBuf[strlen(LineBuf) - 1] = 0x00;
	return 0;
}

void CIniReader::_clearSectionList()
{
	for(_iter_section = sectionlist.begin();_iter_section != sectionlist.end(); _iter_section ++)
	{
		_this_section = (*_iter_section);
		_value_listp = &(_this_section->valuelist);
		for(_iter_value = _value_listp->begin();_iter_value != _value_listp->end(); _iter_value ++)
		{
			_this_value = (*_iter_value);
			delete _this_value;
		}
		_this_section->valuelist.clear();
		delete _this_section;
	}
	sectionlist.clear();
}

int CIniReader::open(const char *IniPath)
{
	FILE *_fp = NULL;
	char _linebuf[1024];
	IDD_Value _suck_value;
	int _rc;

	if(!IniPath) return IDD_ERR_PARAM_INVALID;

	_fp = fopen(IniPath, "r");
    if(_fp==NULL) return IDD_ERR_INIFILE_OPENFAILED;

	_this_section = NULL;
	_this_value = NULL;
	_clearSectionList();

	while(fgets(_linebuf, 1024, _fp)) 
	{
		__removeRN(_linebuf);

		if(0 == strcmp(_linebuf, "[END]")) break;

		if(_linebuf[0] == '\0')
		{
			continue;
		}
		else if(_linebuf[0] == '[')
		{
			//[PartitionedServer]
			if(_this_section)
			{
				sectionlist.push_back(_this_section);
			}
				
			_this_section = new IDD_Section;
			__removeEndMark(_linebuf + 1, ']');
			_snprintf(_this_section->name, sizeof(_this_section->name), "%s", _linebuf + 1);
			continue;
		}

		if(!_this_section) continue; //

		//NotesIniPath=C:\tmp\server1\notes.ini
		_rc = _suckValue(_linebuf, _suck_value.name, _suck_value.value);
		if(_rc != 0) continue;

		_this_value = new IDD_Value;
		_snprintf(_this_value->name, sizeof(_this_value->name), "%s", _suck_value.name);
		_snprintf(_this_value->value, sizeof(_this_value->value), "%s", _suck_value.value);
		_this_section->valuelist.push_back(_this_value);
	}
	if(_this_section)
		sectionlist.push_back(_this_section);
	fclose(_fp);

	return 0;
}

int CIniReader::close()
{
	_clearSectionList();
	return 0;
}

void CIniReader::dumpStdout()
{
	for(_iter_section = sectionlist.begin();_iter_section != sectionlist.end(); _iter_section ++)
	{
		// dump section
		_this_section = (*_iter_section);
		printf("[%s]\n", _this_section->name);

		_value_listp = &(_this_section->valuelist);
		for(_iter_value = _value_listp->begin();_iter_value != _value_listp->end();_iter_value ++)
		{
			_this_value = (*_iter_value);
			printf("%s=%s\n", _this_value->name, _this_value->value);
		}
		printf("\n");
	}
	printf("[END]\n");
}

int CIniReader::_suckValue(char *LineBuf, char *NameBuf, char *ValueBuf)
{
	char *_pt, *_head;
	int _name_size, _value_size;

	_head = LineBuf;
	_pt = strchr(LineBuf, '=');
	if(!_pt) return IDD_ERR_INVALID_VALUE;

	// suck value name
	_snprintf(NameBuf, IDD_VALUE_NAME_SIZE, "%s", LineBuf);
	_name_size = (int)(_pt - _head);
	if(_name_size > IDD_VALUE_NAME_SIZE) return IDD_ERR_VALUE_NAME_OVERSIZE;
	NameBuf[_name_size] = '\0';

	// suck value 
	_snprintf(ValueBuf, IDD_VALUE_SIZE, "%s", _pt + 1);
	_value_size = strlen(_pt + 1);
	if(_value_size > IDD_VALUE_SIZE) return - IDD_ERR_VALUE_OVERSIZE;
	ValueBuf[_value_size] = '\0';

	return 0;
}

int CIniReader::getValue(const char *SectionName, const char *ValueName, char *ValueBuf, int *BufSize)
{
	if(!SectionName || !ValueName || !ValueBuf || !BufSize) return IDD_ERR_PARAM_INVALID;

	for(_iter_section = sectionlist.begin();_iter_section != sectionlist.end(); _iter_section ++)
	{
		_this_section = (*_iter_section);

		if(0 == strcmp(SectionName, _this_section->name))
		{
			_value_listp = &(_this_section->valuelist);
			for(_iter_value = _value_listp->begin();_iter_value != _value_listp->end();_iter_value ++)
			{
				_this_value = (*_iter_value);
				if(0 == strcmp(ValueName, _this_value->name)) 
				{
					_snprintf(ValueBuf, *BufSize, "%s", _this_value->value);
					*BufSize = strlen(ValueBuf);
					return 0;
				}
			}
			return IDD_ERR_VALUE_NOTFOUND;
		}
	}
	return IDD_ERR_SECTION_NOTFOUND;
}

int CIniReader::getValueAt(const char *SectionName, const char *ValueName, char *ValueBuf, int *BufSize, int index)
{
	int _find_index = 0;

	if(!SectionName || !ValueName || !ValueBuf || !BufSize) return IDD_ERR_PARAM_INVALID;

	for(_iter_section = sectionlist.begin();_iter_section != sectionlist.end(); _iter_section ++)
	{
		_this_section = (*_iter_section);

		if(0 == strcmp(SectionName, _this_section->name))
		{
			_value_listp = &(_this_section->valuelist);
			for(_iter_value = _value_listp->begin();_iter_value != _value_listp->end();_iter_value ++)
			{
				_this_value = (*_iter_value);
				if(0 == strcmp(ValueName, _this_value->name)) 
				{
					if(_find_index == index)// Meet the find index
					{
						_snprintf(ValueBuf, *BufSize, "%s", _this_value->value);
						*BufSize = strlen(ValueBuf);
						return 0;
					}
					_find_index ++;
				}
			} // end of for
			return IDD_ERR_VALUE_NOTFOUND;
		}// end of if
	}
	return IDD_ERR_SECTION_NOTFOUND;
}

int CIniReader::getValueCount(const char *SectionName, const char *ValueName)
{
	int _count = 0;

	if(!SectionName || !ValueName) return IDD_ERR_PARAM_INVALID;

	for(_iter_section = sectionlist.begin();_iter_section != sectionlist.end(); _iter_section ++)
	{
		_this_section = (*_iter_section);

		if(0 == strcmp(SectionName, _this_section->name))
		{
			_value_listp = &(_this_section->valuelist);
			for(_iter_value = _value_listp->begin();_iter_value != _value_listp->end();_iter_value ++)
			{
				_this_value = (*_iter_value);
				if(0 == strcmp(ValueName, _this_value->name)) 
				{
					_count ++;
				}
			}
			return _count;
		}
	}
	return IDD_ERR_SECTION_NOTFOUND;
}


int CIniWriter::__removeEndMark(char *LineBuf, char MarkChar)
{
	char *_pt;
	for(_pt = LineBuf + strlen(LineBuf) - 1; _pt >= 0; _pt --)
	{
		if((*_pt) == MarkChar) {
			*_pt = '\0';
			break;
		}
		*_pt = '\0';
	}
	return 0;
}

int CIniWriter::__removeRN(char *LineBuf)
{
	if(LineBuf[strlen(LineBuf) - 1] == 0x0D || LineBuf[strlen(LineBuf) - 1] == 0x0A) LineBuf[strlen(LineBuf) - 1] = 0x00;
	if(LineBuf[strlen(LineBuf) - 1] == 0x0D || LineBuf[strlen(LineBuf) - 1] == 0x0A) LineBuf[strlen(LineBuf) - 1] = 0x00;
	return 0;
}

void CIniWriter::_clearSectionList()
{
	for(_iter_section = sectionlist.begin();_iter_section != sectionlist.end(); _iter_section ++)
	{
		_this_section = (*_iter_section);
		_value_listp = &(_this_section->valuelist);
		for(_iter_value = _value_listp->begin();_iter_value != _value_listp->end(); _iter_value ++)
		{
			_this_value = (*_iter_value);
			delete _this_value;
		}
		_this_section->valuelist.clear();
		delete _this_section;
	}
	sectionlist.clear();
}

int CIniWriter::open(const char *IniPath)
{
	FILE *_fp = NULL;
	char _linebuf[1024];
	IDD_Value _suck_value;
	int _rc;

	if(!IniPath) return IDD_ERR_PARAM_INVALID;

	_fp = fopen(IniPath, "r");
    if(_fp==NULL) return IDD_ERR_INIFILE_OPENFAILED;

	szIniPath = IniPath;

	_this_section = NULL;
	_this_value = NULL;
	_clearSectionList();

	while(fgets(_linebuf, 1024, _fp)) {
		__removeRN(_linebuf);

		if(0 == stricmp(_linebuf, "[END]")) break;

		if(_linebuf[0] == '\0')
		{
			continue;
		}
		else if(_linebuf[0] == '[')
		{
			//[PartitionedServer]
			if(_this_section)
			{
				sectionlist.push_back(_this_section);
			}
				
			_this_section = new IDD_Section;
			__removeEndMark(_linebuf + 1, ']');
			_snprintf(_this_section->name, sizeof(_this_section->name), "%s", _linebuf + 1);
			continue;
		}

		if(!_this_section) continue; //

		//NotesIniPath=C:\tmp\server1\notes.ini
		_rc = _suckValue(_linebuf, _suck_value.name, _suck_value.value);
		if(_rc != 0) continue;

		_this_value = new IDD_Value;
		_snprintf(_this_value->name, sizeof(_this_value->name), "%s", _suck_value.name);
		_snprintf(_this_value->value, sizeof(_this_value->value), "%s", _suck_value.value);
		_this_section->valuelist.push_back(_this_value);
	}
	if(_this_section)
		sectionlist.push_back(_this_section);
	fclose(_fp);

	return 0;
}

int CIniWriter::close()
{
	_clearSectionList();
	return 0;
}

void CIniWriter::dumpStdout()
{
	for(_iter_section = sectionlist.begin();_iter_section != sectionlist.end(); _iter_section ++)
	{
		// dump section
		_this_section = (*_iter_section);
		printf("[%s]\n", _this_section->name);

		_value_listp = &(_this_section->valuelist);
		for(_iter_value = _value_listp->begin();_iter_value != _value_listp->end();_iter_value ++)
		{
			_this_value = (*_iter_value);
			printf("%s=%s\n", _this_value->name, _this_value->value);
		}
		printf("\n");
	}
	printf("[END]\n");
}

int CIniWriter::_suckValue(char *LineBuf, char *NameBuf, char *ValueBuf)
{
	char *_pt, *_head;
	int _name_size, _value_size;

	_head = LineBuf;
	_pt = strchr(LineBuf, '=');
	if(!_pt) return IDD_ERR_INVALID_VALUE;

	// suck value name
	_snprintf(NameBuf, IDD_VALUE_NAME_SIZE, "%s", LineBuf);
	_name_size = (int)(_pt - _head);
	if(_name_size > IDD_VALUE_NAME_SIZE) return IDD_ERR_VALUE_NAME_OVERSIZE;
	NameBuf[_name_size] = '\0';

	// suck value 
	_snprintf(ValueBuf, IDD_VALUE_SIZE, "%s", _pt + 1);
	_value_size = strlen(_pt + 1);
	if(_value_size > IDD_VALUE_SIZE) return - IDD_ERR_VALUE_OVERSIZE;
	ValueBuf[_value_size] = '\0';

	return 0;
}

int CIniWriter::SetValue(const char *SectionName, const char *ValueName, const char *ValueBuf)
{
	if(!SectionName || !ValueName || !ValueBuf) return IDD_ERR_PARAM_INVALID;

	for(_iter_section = sectionlist.begin();_iter_section != sectionlist.end(); _iter_section ++)
	{
		_this_section = (*_iter_section);

		if(0 == strnicmp(SectionName, _this_section->name, strlen(SectionName)))
		{
			_value_listp = &(_this_section->valuelist);
			for(_iter_value = _value_listp->begin();_iter_value != _value_listp->end();_iter_value ++)
			{
				_this_value = (*_iter_value);
				if(0 == strnicmp(ValueName, _this_value->name, strlen(ValueName))) 
				{
					strncpy(_this_value->value, ValueBuf, sizeof(_this_value->value));
					return dumpFile();
				}
			}
			return IDD_ERR_VALUE_NOTFOUND;
		}
	}
	return IDD_ERR_SECTION_NOTFOUND;
}

int CIniWriter::SetValueAt(const char *SectionName, const char *ValueName, const char *ValueBuf, int index)
{
	int _find_index = 0;

	if(!SectionName || !ValueName || !ValueBuf) return IDD_ERR_PARAM_INVALID;

	for(_iter_section = sectionlist.begin();_iter_section != sectionlist.end(); _iter_section ++)
	{
		_this_section = (*_iter_section);

		if(0 == strnicmp(SectionName, _this_section->name, strlen(SectionName)))
		{
			_value_listp = &(_this_section->valuelist);
			for(_iter_value = _value_listp->begin();_iter_value != _value_listp->end();_iter_value ++)
			{
				_this_value = (*_iter_value);
				if(0 == strnicmp(ValueName, _this_value->name, strlen(ValueName))) 
				{
					if(_find_index == index)// Meet the find index
					{
						strncpy(_this_value->value, ValueBuf, sizeof(_this_value->value));
						return dumpFile();
					}
					_find_index ++;
				}
			} // end of for
			return IDD_ERR_VALUE_NOTFOUND;
		}// end of if
	}
	return IDD_ERR_SECTION_NOTFOUND;
}

int CIniWriter::getValueCount(const char *SectionName, const char *ValueName)
{
	int _count = 0;

	if(!SectionName || !ValueName) return IDD_ERR_PARAM_INVALID;

	for(_iter_section = sectionlist.begin();_iter_section != sectionlist.end(); _iter_section ++)
	{
		_this_section = (*_iter_section);

		if(0 == strnicmp(SectionName, _this_section->name, strlen(SectionName)))
		{
			_value_listp = &(_this_section->valuelist);
			for(_iter_value = _value_listp->begin();_iter_value != _value_listp->end();_iter_value ++)
			{
				_this_value = (*_iter_value);
				if(0 == strnicmp(ValueName, _this_value->name, sizeof(ValueName))) 
				{
					_count ++;
				}
			}
			return _count;
		}
	}
	return IDD_ERR_SECTION_NOTFOUND;
}

int CIniWriter::dumpFile()
{
	FILE *_fp = NULL;
	char szIniBak[1024];

	if(!szIniPath) return IDD_ERR_PARAM_INVALID;

	_snprintf(szIniBak, sizeof(szIniBak), "%s.bak", szIniPath);
	std::ifstream ifs(szIniPath, std::ios::in|std::ios::binary);
	std::ofstream ofs(szIniBak, std::ios::out|std::ios::binary|std::ios::trunc);
	ofs<<ifs.rdbuf();

	_fp = fopen(szIniPath, "w");
	if(_fp==NULL) return IDD_ERR_INIFILE_OPENFAILED;

	for(_iter_section = sectionlist.begin();_iter_section != sectionlist.end(); _iter_section ++)
	{
		// dump section
		_this_section = (*_iter_section);
		fprintf(_fp, "[%s]\n", _this_section->name);

		_value_listp = &(_this_section->valuelist);
		for(_iter_value = _value_listp->begin();_iter_value != _value_listp->end();_iter_value ++)
		{
			_this_value = (*_iter_value);
			fprintf(_fp, "%s=%s\n", _this_value->name, _this_value->value);
		}
		fprintf(_fp, "\n");
	}
	fprintf(_fp, "[END]\n");
	
	fclose(_fp);

	return 0;
}


//
//================================================================================================================
//
#ifdef ENABLE_INI_WRITER_UNITEST

int main(int argc, char *argv[])
{
	char _ini_path[] = "c:\\smdsys.ini";
	CIniWriter writer;
	int _value_count;
	int _index;
	int _rc;
	int _buf_size;
	char _value_buf[256];


	writer.open(_ini_path);

	printf("Dump %s...\n", _ini_path);
	writer.dumpStdout();

	printf("\nTest getValueCount...\n");
	_value_count = writer.getValueCount("PartitionedServer", "NotesIniPath");
	if(_value_count < 0)
	{
		printf("getValueCount Err: %s\n", getIDDErrDesc(_value_count));
	}
	else{
		printf("NotesIniPath has : %d \n", _value_count);
	}

	printf("\nTest getValueAt...\n");
	for(_index = 0;_index < _value_count;_index ++)
	{
		_buf_size = sizeof(_value_buf);
		_rc = writer.getValueAt("PartitionedServer", "NotesIniPath", _value_buf, &_buf_size, _index);
		if(_rc != IDD_ERR_ZERO)
		{
			printf("getValueAt Err: %s\n", getIDDErrDesc(_rc));
		}
		else
		{
			printf("NotesIniPath %d : %s \n", _index, _value_buf);
		}
	}

	printf("\nTest getValue...\n");
	_rc = writer.getValue("Domino", "BinaryPath", _value_buf, &_buf_size);
	if(_rc != IDD_ERR_ZERO)
	{
		printf("getValueAt Err: %s\n", getIDDErrDesc(_rc));
	}
	else
	{
		printf("BinaryPath : %s \n", _value_buf);
	}
	

	writer.SetValue("SMDConf", "ProductVersion", "V5.0.0.2233");

	writer.close();
	return 0;
}

#endif


#endif

