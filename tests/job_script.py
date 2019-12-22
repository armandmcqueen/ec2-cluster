import subprocess

def humanize_float(num): return "{0:,.2f}".format(num)
def run(cmd): return subprocess.check_output(cmd, shell=True)


if __name__ == '__main__':
    out = run(f"aws s3 cp --recursive "
              f"s3://armandmcqueen-sports-data/ingestion-raw/weekly_player_gamelogs "
              f"~/data ")

    print(out.decode())

    print("job_script.py complete")

