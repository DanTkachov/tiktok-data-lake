from tasks import add
from time import sleep

result = add.delay(4,5)
sleep(0.001)
print(result.ready())
print(result.collect())