import subprocess

fail_count=0
for i in range(1,11):
    with open(f'{i}.log','+w') as f:
        ret=subprocess.run(['go','test','-race','-run','3B'],stdout=f)
        if ret.returncode!=0:
            print(f'test {i} failed')
            fail_count+=1

if(fail_count==0):
    print("Congrats! You passed all test!")