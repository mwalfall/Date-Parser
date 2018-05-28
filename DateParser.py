import boto3
import botocore
import pandas as pd
import re

from datetime import datetime
from sqlalchemy import create_engine


class AWSs3Service():

    def get_s3_object(self, bucket_name, key, path):
        """
        Downloads an object from AWS S3.
        :param bucket_name: Bucket name
        :param key: The name of the object that is to be downloaded.
        :param path: Local path where the object is to be stored.
        :return: Returns True if the object was downloaded; otherwise False
        """

        s3 = boto3.resource('s3')

        try:
            s3.Bucket(bucket_name).download_file(key, path)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("ERROR: The object '{}' does not exist.".format(key))
                return False

        return True


class AWSDataService:

    def __init__(self):
        db_string = 'postgres://dbuser01:nice1255@postgres01.ct6obuhakepv.us-east-1.rds.amazonaws.com:5432/gdb'
        self.engine = create_engine(db_string)

    def create_db(self):
        db_identifier = 'yourDBID'
        rds = boto3.client('rds')
        try:
            rds.create_db_instance(DBInstanceIdentifier=db_identifier,
                                   AllocatedStorage=200,
                                   DBName='yourdbname',
                                   Engine='postgres',
                                   # General purpose SSD
                                   StorageType='gp2',
                                   StorageEncrypted=True,
                                   AutoMinorVersionUpgrade=True,
                                   MultiAZ=False,
                                   MasterUsername='youruser',
                                   MasterUserPassword='yourpassword',
                                   VpcSecurityGroupIds=['YOUR_SECURITY_GROUP_ID'],
                                   DBInstanceClass='db.m3.2xlarge',
                                   Tags=[{'Key': 'MyTag', 'Value': 'Hawaii'}])
            print 'Starting RDS instance with ID: %s' % db_identifier
        except botocore.exceptions.ClientError as e:
            if 'DBInstanceAlreadyExists' in e.message:
                print 'DB instance %s exists already, continuing to poll ...' % db_identifier
            else:
                raise

        running = True
        while running:
            response = rds.describe_db_instances(DBInstanceIdentifier=db_identifier)

            db_instances = response['DBInstances']
            if len(db_instances) != 1:
                raise Exception('Whoa cowboy! More than one DB instance returned; this should never happen')

            db_instance = db_instances[0]

            status = db_instance['DBInstanceStatus']

            print 'Last DB status: %s' % status

            time.sleep(5)
            if status == 'available':
                endpoint = db_instance['Endpoint']
                host = endpoint['Address']
                # port = endpoint['Port']

                print 'DB instance ready with host: %s' % host
                running = False

    def create_table(self):
        pass

    def drop_db(self):
        pass

    def post(self, df, table_name):
        df.to_sql(table_name, self.engine, if_exists='append', index=False)


class Parser:
    def __init__(self):
        self.aws_service = AWSs3Service()

    def convert_list_to_df(self, doc):
        """
        Given a list convert it to DataFrame containing the following columns:
        * 'Text'
        * 'formatted_date'.
        """
        df = pd.DataFrame(doc, columns=['Text'])
        df['formatted_date'] = pd.Series()
        return df

    def date_sorter(self, df):
        result = pd.Series(df.index)
        return result

    def date_transformer(self, df):
        """
        Applies a lambda to each row in the DataFrame. Each record in the dataset is processed
        once.
        """
        df = df.apply(lambda row: self.process_text(row), axis=1)
        return df

    def get_day(self, day):
        """
        Returns day of month in 'dd' format.
        """
        day = day.replace(',', '').replace('.', '')
        if len(day) == 2:
            return day
        else:
            return '0' + day

    def get_month(self, month):
        """
        Returns string with month in 'mm' format.
        """
        month = month.replace(',', '').replace('.', '')
        if len(month) == 2:
            return month
        else:
            return '0' + month


    def get_month_from_name(self, month):
        """
        Given an alpha description of a month a string containing the numeric
        equivalent is returned.
        """

        result = re.match(r'[Jj]an[a-z,.]*', month)
        if result is not None:
            return '01'
        result = re.match(r'[Ff]eb[a-z,.]*', month)
        if result is not None:
            return '02'
        result = re.match(r'[Mm]ar[a-z,.]*', month)
        if result is not None:
            return '03'
        result = re.match(r'[Aa]pr[a-z,.]*', month)
        if result is not None:
            return '04'
        result = re.match(r'[Mm]ay[a-z,.]*', month)
        if result is not None:
            return '05'
        result = re.match(r'[Jj]un[a-z,.]*', month)
        if result is not None:
            return '06'
        result = re.match(r'[Jj]ul[a-z,.]*', month)
        if result is not None:
            return '07'
        result = re.match(r'[Aa]ug[a-z,.]*', month)
        if result is not None:
            return '08'
        result = re.match(r'[Ss]ep[a-z,.]*', month)
        if result is not None:
            return '09'
        result = re.match(r'[Oo]ct[a-z,.]*', month)
        if result is not None:
            return '10'
        result = re.match(r'[Nn]ov[a-z,.]*', month)
        if result is not None:
            return '11'
        result = re.match(r'[Dd]ec[a-z,.]*', month)
        if result is not None:
            return '12'

        return None

    def get_year(self, year):
        """
        Returns year in 'yyyy' format.
        """
        if len(year) == 4:
            return year
        else:
            return '19{}'.format(year)

    def extract_data(self, use_aws=False, bucket_name=None, key=None, path='dates.txt'):
        """
        Extracts data from 'dates.txt' file. This file consists of clinic patient notes
        that contain dates that are in different formats.
        """
        doc = list()

        if use_aws:
            is_success = self.aws_service.get_s3_object(bucket_name=bucket_name,
                                                        key=key,
                                                        path=path)
        if not is_success:
            return None

        with open(path) as data_file:
            for line in data_file:
                doc.append(line)

        return doc

    def process_text(self, row):
        """
        Extracts the date contained in the 'Text' field and stored a formatted date in the
        'formatted_date' field. Where applicable the method processes data from most specific
        to least specific.
        """
        # Check for date in the format similar to mm/dd/yyyy.
        tmp = re.search(r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}', row['Text'])
        if tmp is not None:
            try:
                value = tmp.group()
                if '-' in value:
                    mdy = tmp.group().split('-')
                else:
                    mdy = tmp.group().split('/')
                mm = mdy[0]
                if len(mdy[0]) == 1:
                    mm = '0' + mdy[0]
                dd = mdy[1]
                if len(mdy[1]) == 1:
                    dd = '0' + mdy[1]
                yyyy = mdy[2]
                if len(mdy[2]) == 2:
                    yyyy = '19' + mdy[2]
                row['formatted_date'] = self.get_date_object('{}/{}/{}'.format(mm, dd, yyyy))
            except Exception:
                print('ERROR_0 > {} -- {}'.format(tmp.group(), row['Text']))
            return row

        # Check for date in 'month year' format. For example, 'Feb 2009', 'June 97'.
        tmp = re.search(r'(\d{1,2} ([Jj]an|[Ff]eb|[Mm]ar|[Aa]pr|[Mm]ay|[Jj]un|[Jj]ul|[Aa]ug|'
                        r'[Ss]ep|[Oo]ct|[Nn]ov|[Dd]ec)[a-z,.]* \d{2,4})', row['Text'])
        if tmp is not None:
            try:
                dmy = tmp.group().split(' ')
                dd = self.get_day(dmy[0].strip())
                mm = self.get_month_from_name(dmy[1].strip())
                yyyy = self.get_year(dmy[2].strip())
                row['formatted_date'] = self.get_date_object('{}/{}/{}'.format(mm, dd, yyyy))
            except Exception:
                print('ERROR_1 > {} -- {}'.format(tmp.group(), row['Text']))
            return row

        # Check for date in 'month dd, yyyy'. For example, Jan. 20, 2015.
        tmp = re.search(r'(([Jj]an|[Ff]eb|[Mm]ar|[Aa]pr|[Mm]ay|[Jj]un|[Jj]ul|[Aa]ug|[Ss]ep|'
                        r'[Oo]ct|[Nn]ov|[Dd]ec)[a-z,.]*[\s.-]+\d{1,2}[\s,-]+\d{2,4})', row['Text'])
        if tmp is not None:
            try:
                value = tmp.group()
                if '-' in value:
                    mdy = tmp.group().split('-')
                else:
                    mdy = tmp.group().split(' ')
                dd = self.get_day(mdy[1].strip())
                mm = self.get_month_from_name(mdy[0].strip())
                yyyy = self.get_year(mdy[2].strip())
                row['formatted_date'] = self.get_date_object('{}/{}/{}'.format(mm, dd, yyyy))
            except Exception:
                print("ERROR_3 > ", tmp.group())
            return row

        # Find date similar to June, year. This will be stored as 'mm/01/year'.
        tmp = re.search(r'(([Jj]an|[Ff]eb|[Mm]ar|[Aa]pr|[Mm]ay|[Jj]un|[Jj]ul|[Aa]ug|[Ss]ep|'
                        r'[Oo]ct|[Nn]ov|[Dd]ec)[a-z,.]* \d{4})', row['Text'])
        if tmp is not None:
            try:
                my = tmp.group().split(' ')
                mm = self.get_month_from_name(my[0].strip())
                yyyy = self.get_year(my[1].strip())
                row['formatted_date'] = self.get_date_object('{}/01/{}'.format(mm, yyyy))
            except Exception:
                print("ERROR_4 > ", tmp.group())
            return row

        # Find date similar to 'month/year' format. This will be stored as mm/01/yyyy.
        tmp = re.search(r'(^| )(\d{1,2}[/]\d{2,4})', row['Text'])
        if tmp is not None:
            try:
                my = tmp.group().split('/')
                mm = self.get_month(my[0].strip())
                yyyy = self.get_year(my[1].strip())
                row['formatted_date'] = self.get_date_object('{}/01/{}'.format(mm, yyyy))
            except Exception:
                print("ERROR_5 > ", tmp.group())
            return row

        # Find a year. This will be stored as: '01/01/1998'
        tmp = re.search(r'(^| )(\d{4})', row['Text'])
        if tmp is not None:
            try:
                yyyy = tmp.group().strip()
                row['formatted_date'] = self.get_date_object('01/01/{}'.format(self.get_year(yyyy)))
            except Exception:
                print("ERROR_6 > ", tmp.group())
            return row

        # Find a year. This will be stored as: '01/01/1997'.
        tmp = re.search(r'(\d{4})', row['Text'])
        if tmp is not None:
            try:
                yyyy = tmp.group().strip()
                row['formatted_date'] = self.get_date_object('01/01/{}'.format(self.get_year(yyyy)))
            except Exception:
                print("ERROR_7 > ", tmp.group())
            return row

        # Unable to find a matching date format.
        row['formatted_date'] = self.get_date_object('01/01/1900')
        return row

    def get_date_object(self, str_date):
        """
        Given a date in 'mm/dd/yyyy' format returns the associated date object.
        """
        return datetime.strptime(str_date, '%m/%d/%Y')


def main():
    # Extract documents.
    parser = Parser()
    doc = parser.extract_data(use_aws=True,
                              bucket_name='mlw226',
                              key='dates.txt',
                              path='downloaded.txt')

    if doc is None:
        print('Documents not found. Processing halted...')

    # Convert to pandas DataFrame.
    df = parser.convert_list_to_df(doc)

    # Read Text field to transform dates and place in 'formatted_date' field.
    df = parser.date_transformer(df)
    df = df.sort_values(by=['formatted_date'])

    df.columns = ['text', 'date']
    data_service = AWSDataService()
    data_service.post(df=df, table_name='notes')


if __name__ == "__main__":
    main()
