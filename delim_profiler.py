import unicodecsv as csv
from itertools import islice
from os import listdir
from os.path import isfile, join
from hyperloglog import HyperLogLog
from dateutil.parser import parse,parserinfo
from tabulate import tabulate
import datetime




DELIMITERS= ';\t,|'

TYPES={'unicode':{'ansi':'String','weight':10}
       , 'str':{'ansi':'String','weight':10}
       ,'string':{'ansi':'String','weight':10}
       , 'float':{'ansi':'Decimal','weight':7}
       ,'long':{'ansi':'BigInt','weight':5}
       , 'int':{'ansi':'Integer','weight':3}
       ,'datetime':{'ansi':'DateTime','weight':1}
       }





def find_type(a):

    var_type=None

    #### TRY TO PARSE AS A DECIMAL
    if a.count('.')==1:

        try:

            var_type=type(float(a))

        except ValueError:

            pass



    ######## TRY TO PARSE AS A DATE 
    if var_type==None and len(a)>=7:

        try:

            var_type = type(parse(a))

        except:

            try:

                var_type=type(datetime.datetime.strptime(a, '%m%d%Y'))

            except:

                pass

    ####### TRY TO PARSE AS AN INT
    if var_type ==None:

        try:

            var_type = type(int(a))

            ### test for numbers that should be treated as a string

            if len(a)>1 and a[0]=='0':

                var_type=type('')


        except ValueError:

            var_type = type(a)


    if var_type == None:
        return 'string'


    return var_type.__name__




def get_csv(infile):



    sniff_range = 4096

    sniffer = csv.Sniffer()
    

    dialect = sniffer.sniff(infile.read(sniff_range), 
                            delimiters=DELIMITERS)
        
    infile.seek(0)

        # Sniff for header
    header = sniffer.has_header(infile.read(sniff_range))

    infile.seek(0)

        # get the csv reader
    reader = csv.reader(infile, dialect)

    firstrow=next(reader)

    colnames=[]



    for i,h in enumerate(firstrow):

        if len(h)>0 and header:

            colnames.append(h)

        else:

            colnames.append('COLUMN{}'.format(i+1))
    

    if not header:

        infile.seek(0)



    return (reader, colnames)






def profile (csvreader, colnames, samplesize=1000):

    # set the chunksize to be read from the file equal to the 
    # number of records that are being profiled when
    # the profile size is less than the chunk size
    # this reduces unnecessary reads on the file



    stats={}

    # build a dictionary with the initial set of statistics

    for i,col in enumerate(colnames):
        stats[i]={'Name':col,'TypeList':[],'MaxVal':None,'MinVal':None,'MaxLen':None,'DecPlaces':None, 'Nulls':False, 'Cardinality':HyperLogLog(0.01)}



    rec_cnt=0

    for row in csvreader:

        if rec_cnt == samplesize:
            break

        rec_cnt+=1

        for i,col in enumerate(row):

            if col:

                t=find_type(col)

                if t not  in stats[i]['TypeList']:

                    stats[i]['TypeList'].append(t)

                if col>stats[i]['MaxVal']:

                    stats[i]['MaxVal']=col

                if col<stats[i]['MinVal'] or stats[i]['MinVal'] == None:

                    stats[i]['MinVal']=col

                v_len=len(col)

                if v_len>stats[i]['MaxLen']:

                    stats[i]['MaxLen']=v_len

                if t=='float':

                    dec_places=len(str(col).split('.')[1])
                #print str(col)
                    if stats[i]['DecPlaces']<dec_places:

                        stats[i]['DecPlaces']=dec_places

                try:

                    stats[i]['Cardinality'].add(col)

                except:

                    pass

            else:

                stats[i]['Nulls']=True


    filestats=[]

    for key,v in enumerate(stats):

        val=stats[key]

        # set default type to string when no type was found 
        if not val['TypeList']:
            val['TypeList'].append('string')

        types_found=[TYPES[t] for t in val['TypeList']]
        recommend_type = max(types_found, key=lambda x:x['weight'])




        filestats.append([val['Name'], ','.join(val['TypeList']), val['MaxLen'], val['MinVal'],val['MaxVal']
                           , val['DecPlaces'], val['Nulls'], int(val['Cardinality'].card())
                           , recommend_type['ansi']])



    return rec_cnt, filestats



def print_stats(filename, file_stats, rec_cnt):
    
    stat_names=['Name','PossibleTypes','MaxLen','MinVal','MaxVal','DecPlaces','Nulls','Cardinality', 'RecommendedType']
    print '#'*25+filename+'#'*25
    print tabulate(file_stats,stat_names)
    print '\n'
    print 'Total Records Profiled = {}'.format(rec_cnt)
    print '\n'
    



path='data'

datafiles=[join(path,f) for f in listdir(path) if isfile(join(path, f))]

for f in datafiles:

    with open(f, 'r') as infile :
        
        reader,colnames=get_csv(infile)

        rec_cnt, filestats=profile(reader, colnames,1000)

        print_stats(f, filestats, rec_cnt)
