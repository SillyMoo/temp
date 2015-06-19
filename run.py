import sys
import os
import time
import gzip
import shutil
from ftplib import FTP

if len(sys.argv) < 5:
    print("Syntax: %s <ip_addr> <config file> <working directory> <output directory>", (sys.argv[0]))
    print("    <ip_addr>            The ip address of the server to retrieve logs from")
    print("    <config file>        The file to store configuration in.")
    print("    <working directory>  The directory to donload zipped files into before")
    print("                         unzipping them and importing them.")
    print("    <output directory>   The directory to store unzipped files into.")
    quit()

ip_addr = sys.argv[1]
cfg_file = sys.argv[2]
work_dir = sys.argv[3]
out_dir = sys.argv[4]


def process_files(file_list, last_log_date, last_log_index):
    oldest_log_date = last_log_date
    oldest_log_index = last_log_index
    log_index_str=""
    download_list = []

    for i, name in enumerate(file_list):
        try:
            tmp1, tmp2, log_date_str, log_index_str, ext = name.split(".")
            log_date =  time.mktime(time.strptime(log_date_str, "%Y-%m-%d"))
        except:
            print("Failed to parse filename '%s'." % (name))
            pass
        else:
            log_index = int(log_index_str)

            if (log_date > last_log_date) or ((log_date == last_log_date) and (log_index > last_log_index)):
                download_list.append(name)

            if (log_date > oldest_log_date) or ((log_date == oldest_log_date) and (log_index > oldest_log_index)):
                oldest_log_date = log_date
                oldest_log_index = log_index

    try:
        cfg = open(cfg_file, "w")
    except:
        print("Failed to open file '%s' to write last log date." % cfg_file)
        quit()
    else:
        cfg.write("last_log = %s %s" % (oldest_log_date, oldest_log_index))
        cfg.close()

    return download_list


while 1:
    last_log_date_str=""
    last_log_index_str=""
    last_log_date = 0
    last_log_index = 0
    cfg_line = ""

    try:
        cfg = open(cfg_file, "r")
    except:
        print("First run - importing all log files.")
    else:
        cfg_line = cfg.readline()
        cfg.close()

    if not cfg_line:
        print("Empty config file - importing all log files.")
    else:
        try:
            label, equals, last_log_date_str, last_log_index_str = cfg_line.split(" ")
        except:
            print("Invalid config file. Please fix it.")
            quit()
        else:
            if not label == "last_log":
                print("Invalid config file. Please fix it.")
                quit()
            else:
                last_log_date = float(last_log_date_str)
                last_log_index = int(last_log_index_str)                

    print ("Connecting to ftp server to check for new logs.")

    zipfile_list = []

    try:
        ftp = FTP(ip_addr, "hackit", "hackit!")
    except ftplib.all_errors:
        print("Error connecting to FTP server.")
    else:
        print ("Successfully connected to ftp server.")

        file_list = []
        download_list = []

        try:
            ftp.cwd("logs/cmdc-archive")
            file_list = ftp.nlst()
        except:
            print("Failed to get directory listing.")

        if not file_list:
            print("No files found.")
        else:
            download_list = process_files(file_list, last_log_date, last_log_index)

        if download_list:
            for i, name in enumerate(download_list):
                try:
                    out_path = os.path.join(work_dir, name)
                    out_file = open(out_path, "wb")
                except:
                    print("Failed to open temporary file for log '%s'." % (name))
                else:
                    try:
                        print("Downloading '%s'." % (name))
                        ftp.retrbinary("RETR " + name , out_file.write)
                    except:
                        print("Failed to download file '%s'." % (name))
                    else:
                        zipfile_list.append(out_path)

                    out_file.close()

        ftp.quit()

    if zipfile_list:
        for i, zip_path in enumerate(zipfile_list):
            try:
                zip_file = gzip.open(zip_path)
            except:
                print("Unable to open zip file '%s'." % (zip_path))
            else:
                base_name, ext = os.path.splitext(os.path.basename(zip_path));
                out_path = os.path.join(out_dir, base_name + ".log")
                try:
                    out_file = open(out_path, "wb")
                except:
                    print("Unable to open output file for zip file '%s'." % (zip_path))
                else:
                    print("Unzipping '%s'." % (base_name))
                    shutil.copyfileobj(zip_file, out_file)
                    out_file.close()

                zip_file.close()

            os.remove(zip_path)

    print("Finished log check.")
    time.sleep(10)
