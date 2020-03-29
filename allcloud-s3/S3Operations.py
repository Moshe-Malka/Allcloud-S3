import logging
import boto3
import re
import pandas as pd
import sys

class S3Operations:
    def __init__(self, **kwargs):
        self.session = boto3.Session(**kwargs)
        self._s3_resource = self.session.resource('s3')
        self._s3_client = self.session.client('s3')
        self.region_name = kwargs.get('region_name', 'us-east-1')

        self.logger = logging.getLogger('allcloud-s3-package')
        self.logger.setLevel(logging.DEBUG)

        self.allowd_signs = ['>', '<']
    
    def isnotebook(self):
        try:
            get_ipython()
            return True
        except Exception:
            return False

    def humanbytes(self, _bytes):
        '''Return the given bytes as a human friendly KB, MB, GB, or TB string'''
        _bytes = float(_bytes)
        kb = float(1024)
        mb = float(kb ** 2) # 1,048,576
        gb = float(kb ** 3) # 1,073,741,824
        tb = float(kb ** 4) # 1,099,511,627,776

        if _bytes < kb:
            return '{0} {1}'.format(B,'Bytes' if 0 == B > 1 else 'Byte')
        elif kb <= _bytes < mb:
            return f'{(_bytes/kb):.2f} KB'
        elif mb <= _bytes < gb:
            return f'{(_bytes/mb):.2f} MB'
        elif gb <= _bytes < tb:
            return f'{(_bytes/gb):.2f} GB'
        elif tb <= _bytes:
            return f'{(_bytes/tb):.2f} TB'
        else: return '<-Error->'

    def filter_by_pattern(self, _list, pat, key=None):
        try:
            pat = pat.replace('*', '.*')
            reg = re.compile(pat)
            if key:
                return list(filter(lambda x: bool(re.search(reg, x[key])), _list))
            else:
                return list(filter(lambda x: bool(re.search(reg, x)), _list))
        except Exception as e:
            self.logger.debug(f'Error trying to filter list by pattren : {pat}.\n{e}')

    def filter_by_size(self, _list, size_str):
        try:
            size = size_str[1:]
            if size_str[0] == '>':
                return list(filter(lambda x: x['Size'] > size, _list))
            elif size_str[0] == '<':
                return list(filter(lambda x: x['Size'] > size, _list))
            else:
                self.logger.error(f'Error trying to filter by size - invalid sign : {size_str}.')
        except Exception as e:
            self.logger.debug(f'Error trying to filter by size.\n{e}')

    def create_bucket(self, bkt_name, region=None):
        try:
            if not region: region = self.region_name
            reaponse = self._s3_resource.create_bucket( Bucket=bkt_name,CreateBucketConfiguration={ 'LocationConstraint': region } )
            self.logger.info(response)
        except Exception as e:
            self.logger.debug(f'Error trying to create Bucket with name {bkt_name} - {e}')

    def list_buckets(self, pat):
        try:
            response = self._s3_client.list_buckets()['Buckets']
            self.logger.info(response)
            buckets = [x['Name'] for x in response]
            return self.filter_by_pattern(buckets, pat)
        except Exception as e:
            self.logger.debug(f'Error trying to list Buckets with pattren {pat} - {e}')

    def list_bucket_content(self, bkt_name, fname_pat=None, size_str=None):
        try:
            response = self._s3_client.list_objects_v2( Bucket=bkt_name )['Contents']
            if len(response) == 0: return []
            files = [ { 'Key' : x['Key'], 'Size' : x['Size']} for x in response]
            if fname_pat:
                files = self.filter_by_pattern(files, fname_pat, 'Key')
            if size_str:
                if size_str[0] not in self.allowd_signs: raise Exception('Error - Unknow/no sign in size argument.')
                files = self.filter_by_size(filterd, size_str)
            formmated = [ { 'Key' : x['Key'], 'Size' : self.humanbytes(x['Size'])} for x in files ]
            return formmated
        except Exception as e:
            self.logger.debug(f'Error trying to list Bucket content with filters: filename : {fname_pat}, size : {size_str}.\n{e}')
        
    def preview_file(self, bkt_name, fname, limit=None):
        try:
            obj = self._s3_client.get_object(Bucket= bkt_name, Key= fname)
            df = pd.read_csv(obj['Body'].read())
            if limit:
                df = df.iloc[:limit]
            with pd.option_context('display.max_rows', None, 'display.max_columns', None):
                if isnotebook(): display(df)
                else: print(df)
        except Exception as e:
            self.logger.debug(f'Error - failed to preview file - {fname}')

    def copy(self, from_bkt, to_bkt, from_key, to_key):
        try:
            source = { 'Bucket': from_bkt , 'Key': from_key }
            target = _s3_resource.Bucket(to_bkt).Object(to_key)
            target.copy(source)
            self.logger.info('Succesfully copied file.')
        except Exception as e:
            self.logger.debug(f'Error - failed to copy file/s.\n{e}')

if __name__ == '__main__':
    s3_actions = S3Operations()


