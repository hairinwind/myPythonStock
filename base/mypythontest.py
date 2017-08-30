'''
This is the file to practise python
'''

def fibon(n):
    print('testing generator...')
    a = b = 1
    for i in range(n):
        yield a
        a, b = b, a + b
        

def echo(value=None):
    while True:
        value = (yield value)
        print("the value is", value)
        value += 1
        
if __name__ == '__main__':      
#     for x in fibon(10):
#         print(x)
    
    g = echo(100)
    valueFromYeild = next(g)
    print("value From Yield", valueFromYeild)
    sendReturn = g.send(27)
    print("sendReturn", sendReturn)
    
    print("send in 5")
    sendReturn = g.send(5)
    print("sendReturn", sendReturn)
    
