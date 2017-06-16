
'''
Transform traffic count files so that they can be
inserted into ArcSDE database

TODO:
- logging
- move processed files to 'processed' folder

'''
import os
import csv
import pdb
import hashlib
import logging
import arrow
import secrets

now = arrow.now()
now_s = now.format('YYYY_MM_DD')

log_directory = secrets.LOG_DIRECTORY
logfile = '{}/traffic_count_pub_{}.log'.format(log_directory, now_s)
logging.basicConfig(filename=logfile, level=logging.INFO)
logging.info('START AT {}'.format(str(now)))


# root_dir = secrets.TIMEMARK_DIRECTORY 
root_dir = 'G:\\Verdi\\Traffic Counts\\TimeMarkGIS'

# out_dir = secrets.FME_DIRECTORY
out_dir = 'H:\\ATD_BSA'

# processed_dir = root_dir + '/processed'
processed_dir = 'H:\\ATD_BSA'


vol_file_match_str = 'VOL.CSV'
# outdir = fme_source_files_dest
outdir = 'H:\\ATD_BSA\\del'

fieldnames = ['COUNT_DATETIME', 'COUNT_CHANNEL', 'CHANNEL', 'COUNT_TOTAL', 'DATA_FILE', 'COUNT_ID', 'SITE_CODE']
directions = ['NB', 'EB', 'WB', 'SB']

# os.makedirs(path, exist_ok=True)
def getFile(path):
    print(path)
    with open(path, 'r') as in_file:
        for i, line in enumerate(in_file):
            if i == 0:
                data_file = line.split(',')[1].strip('\'')

            if i == 1: 
                site_code = line.split(',')[1].strip('\'')

            if 'Date,Time' in line:
                reader = csv.DictReader([line] + in_file.readlines())
                return ([row for row in reader], reader.fieldnames, data_file, site_code)
        else:
            print('can\'t find header row')
            raise Exception

    return 'bob'


def splitRowsByDirection(rows):
    new_rows = []
    
    for row in rows:
        date = row['Date']
        time = row['Time']

        datetime = parseDateTime(date, time)
        
        if 'Total' in row.keys():  #  only files with bi-directional data will have a total
            total = row['Total']
        else:
            total = None
        
        for d in directions:
            if d in row.keys():
                new_row = {}
                new_row['CHANNEL'] = d
                new_row['COUNT_DATETIME'] = datetime
                new_row['COUNT_CHANNEL'] = row[d]

                if (total):
                    new_row['COUNT_TOTAL'] = total
                else:
                    new_row['COUNT_TOTAL'] = new_row['COUNT_CHANNEL']

                new_rows.append(new_row)

    return new_rows

def parseDateTime(d, t):
    dt = '{} {} {}'.format(d, t, 'US/Central')
    return arrow.get(dt, 'M/D/YYYY h:mm A ZZZ').format()


def appendKeyVal(rows, key, val):
    for row in rows:
        row[key] = val
    return rows


def createRowIDs(rows, hash_fields):
    hasher = hashlib.sha1()
    for row in rows:
        in_str = ''.join([row[q] for q in hash_fields])
        hasher.update(in_str.encode('utf-8'))
        row['COUNT_ID'] = hasher.hexdigest()

    return rows


for root, dirs, files in os.walk(root_dir):
    for name in files:
        if 'VOL.CSV' in name.upper() and 'PROCESSED' not in root.upper():
            vol_file = os.path.join(root, name)
            move_file = os.path.join(root, 'processed', name)

            file_data = getFile(vol_file)
            
            rows = file_data[0]
            data_file = file_data[2]
            site_code = file_data[3]
            
            rows = splitRowsByDirection(rows)
            rows = appendKeyVal(rows, 'DATA_FILE', data_file)
            rows = appendKeyVal(rows, 'SITE_CODE', site_code)
            rows = createRowIDs(rows, ['COUNT_DATETIME', 'DATA_FILE', 'COUNT_CHANNEL'])
            # https://stackoverflow.com/questions/34771268/md5-hashing-a-csv-with-python
            out_path = os.path.join(outdir, name)
            
            with open(out_path, 'w', newline='\n') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)

            #  os.rename(vol_file, move_file)
            pdb.set_trace()
            print(out_path)
            #  move processed file to processed dir
        else:
            continue