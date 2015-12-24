#-*- coding: cp936 -*-
global a

def a():
    global a
    a = 2
    a += 1
    print a
 #注意这里没有使用return a 
def do():
    global a
    a()
 #并把a方法的value（a）进行运算
#我们写一个main函数来调用这个do的过程
if __name__ == "__main__":
    global a
    do()
    print a
#我们在Pytho