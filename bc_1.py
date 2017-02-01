#  create ATD dropbox account and share file there
#  get **last month's** trip data from dropbox folder
import dropbox
import arrow
from secrets



#  init dropbox client
access_token = secrets.DROPBOX_BCYCLE_TOKEN
client = dropbox.client.DropboxClient(access_token)



############
#  get latest trip data
############

root = 'austinbcycletripdata'  #  note the lowercase-ness of this path 

one_month_ago = arrow.now().replace(months=-1)

year = one_month_ago.format('YYYY')

month = one_month_ago.format('MM')

current_file = 'TripReport-{}{}.csv'.format(month, year)

path = '/{}/{}/{}'.format(root, year, current_file)

f, metadata = client.get_file_and_metadata(path)

with open(current_file, 'wb') as out:
    out.write(f.read())



############
#  get latest station ('kiosk') list
############
station_filename = '50StationPlusOld-LongLatInfo.csv'

path = '/{}/{}'.format(root, station_filename)





