from tasks import *
from utils import *

if __name__ == '__main__':
    res1 = task1()

    if res1:
        res2 = task2()

        if res2:
            res3 = task3()

            if res3:
                print("\n\nSuccessfully run the pipeline !")