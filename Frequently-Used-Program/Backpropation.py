import subprocess
import sys

def Backpropation_akshare():
    try:
        version = input("请输入要回退到的akshare版本号：")
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", f"akshare=={version}"
        ])
        print("akshare 回退成功！")
    except subprocess.CalledProcessError as e:
        print(f"回退失败：{e}")

if __name__ == "__main__":
    Backpropation_akshare()