import subprocess, sys
def run(cmd):
    print(f"=== Running: {cmd} ===")
    r=subprocess.run(cmd, shell=True)
    if r.returncode!=0: sys.exit(r.returncode)
def main():
    run("python src/ingest.py")
    run("python src/transform_load.py")
    run("python src/modeling.py")
    print("Pipeline completed successfully.")
if __name__=="__main__":
    main()
