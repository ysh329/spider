#-*- coding: cp936 -*-
global a

def a():
    global a
    a = 2
    a += 1
    print a
 #ע������û��ʹ��return a 
def do():
    global a
    a()
 #����a������value��a����������
#����дһ��main�������������do�Ĺ���
if __name__ == "__main__":
    global a
    do()
    print a
#������Pytho