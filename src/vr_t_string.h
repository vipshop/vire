#ifndef _VR_T_STRING_H_
#define _VR_T_STRING_H_

void setGenericCommand(client *c, int flags, robj *key, robj *val, robj *expire, int unit, robj *ok_reply, robj *abort_reply);
void setCommand(client *c);
void setnxCommand(client *c);
void setexCommand(client *c);
void psetexCommand(client *c);
int getGenericCommand(client *c);
void getCommand(client *c);
void getsetCommand(client *c);
void setrangeCommand(client *c);
void getrangeCommand(client *c);
void mgetCommand(client *c);
void msetGenericCommand(client *c, int nx);
void msetCommand(client *c);
void msetnxCommand(client *c);
void incrDecrCommand(client *c, long long incr);
void incrCommand(client *c);
void decrCommand(client *c);
void incrbyCommand(client *c);
void decrbyCommand(client *c);
void incrbyfloatCommand(client *c);
void appendCommand(client *c);
void strlenCommand(client *c);

#endif
