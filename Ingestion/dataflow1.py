import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, WorkerOptions


region='us-central1'
runner='DataflowRunner'
project='viraj-patil-bootcamp'

options = PipelineOptions(
runner=runner,
region=region,
project=project
)

options.view_as(StandardOptions).runner = 'DataflowRunner'
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'viraj-patil-bootcamp'
google_cloud_options.job_name = 'batch-google-cloud-storage-to-bigquery'
worker_options = options.view_as(WorkerOptions)
worker_options.network = 'vpc-bootcamp'
worker_options.subnetwork = 'regions/us-central1/subnetworks/subnet1'

worker_options.machine_type = 'n1-standard-2'
worker_options.max_num_workers = 3

pipeline = beam.Pipeline(options=options)
lines = pipeline | 'ReadFile' >> beam.io.ReadFromText('gs://viraj-patil-dfpart/f100k.csv', skip_header_lines=1)

def parse_csv(line):
    import datetime
    fields = line.split(',')
    parsed_fields = {}
    for i in range(len(fields)):
        field = fields[i] 
        parsed_field = None
        if field.startswith(('True', 'False')):
            try:
                parsed_field = bool(field)
            except ValueError:
                parsed_field = None
        elif field.isdigit():
            parsed_field = int(field)
        elif field.replace('.', '').isdigit():
            parsed_field = float(field)
        else:
            res1 = False
            res = False
            try:
                res = bool(datetime.datetime.strptime(field, '%y-%m-%d').date())
            except ValueError:
                try:
                    res1 = bool(datetime.datetime.strptime(field, '%y-%m-%d %H:%M:%S'))
                except ValueError:
                    parsed_field = field
            if res == True:
                parsed_field = datetime.datetime.strptime(field, '%y-%m-%d').date()
            elif res1 == True:
                parsed_field = datetime.datetime.strptime(field, '%y-%m-%d %H:%M:%S')
            else:
                parsed_field = None

        if parsed_field is not None:
            parsed_fields['col_{}'.format(i)] = parsed_field
        else:
            parsed_fields['col_{}'.format(i)] = None
    return parsed_fields



# def parseCSV(line):
#     fields = line.split(',')
#     if fields[0] != 'col_0':
#         return {'col_0': int(fields[0]), 'col_1': float(fields[1]), 'col_2': fields[2], 'col_3': bool(fields[3]),
#         'col_4': datetime.datetime.strptime(fields[4], '%y-%m-%d').date(), 'col_5': datetime.datetime.strptime(fields[5], '%y-%m-%d %H:%M:%S'), 'col_6': int(fields[6]),
#         'col_7': float(fields[7]), 'col_8': fields[8], 'col_9': bool(fields[9]), 'col_10': datetime.datetime.strptime(fields[10], '%y-%m-%d').date(),
#         'col_11': datetime.datetime.strptime(fields[11], '%y-%m-%d %H:%M:%S'), 'col_12': int(fields[12]), 'col_13': float(fields[13]), 'col_14': fields[14],
#         'col_15': bool(fields[15]), 'col_16': datetime.datetime.strptime(fields[16], '%y-%m-%d').date(), 'col_17': datetime.datetime.strptime(fields[17], '%y-%m-%d %H:%M:%S'),
#         'col_18': int(fields[18]), 'col_19': float(fields[19]), 'col_20': fields[20], 'col_21': bool(fields[21]),
#         'col_22': datetime.datetime.strptime(fields[22], '%y-%m-%d').date(), 'col_23': datetime.datetime.strptime(fields[23], '%y-%m-%d %H:%M:%S'), 'col_24': int(fields[24]),
#         'col_25': float(fields[25]), 'col_26': fields[26], 'col_27': bool(fields[27]), 'col_28': datetime.datetime.strptime(fields[28], '%y-%m-%d').date(),
#         'col_29': datetime.datetime.strptime(fields[29], '%y-%m-%d %H:%M:%S'), 'col_30': int(fields[30]), 'col_31': float(fields[31]), 'col_32': fields[32],
#         'col_33': bool(fields[33]), 'col_34': datetime.datetime.strptime(fields[34], '%y-%m-%d').date(), 'col_35': datetime.datetime.strptime(fields[35], '%y-%m-%d %H:%M:%S'),
#         'col_36': int(fields[36]), 'col_37': float(fields[37]), 'col_38': fields[38], 'col_39': bool(fields[39]),
#         'col_40': datetime.datetime.strptime(fields[40], '%y-%m-%d').date(), 'col_41': datetime.datetime.strptime(fields[41], '%y-%m-%d %H:%M:%S'), 'col_42': int(fields[42]),
#         'col_43': float(fields[43]), 'col_44': fields[44], 'col_45': bool(fields[45]), 'col_46': datetime.datetime.strptime(fields[46], '%y-%m-%d').date(),
#         'col_47': datetime.datetime.strptime(fields[47], '%y-%m-%d %H:%M:%S'), 'col_48': int(fields[48]), 'col_49': float(fields[49])}

rows = lines | 'ParseCSV' >> beam.Map(parse_csv)
table_schema = bigquery.TableSchema()
table_schema.fields.append(bigquery.TableFieldSchema(name='col_0', type='INTEGER'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_1', type='FLOAT'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_2', type='STRING'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_3', type='BOOLEAN'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_4', type='DATE'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_5', type='TIMESTAMP'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_6', type='INTEGER'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_7', type='FLOAT'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_8', type='STRING'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_9', type='BOOLEAN'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_10', type='DATE'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_11', type='TIMESTAMP'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_12', type='INTEGER'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_13', type='FLOAT'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_14', type='STRING'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_15', type='BOOLEAN'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_16', type='DATE'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_17', type='TIMESTAMP'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_18', type='INTEGER'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_19', type='FLOAT'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_20', type='STRING'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_21', type='BOOLEAN'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_22', type='DATE'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_23', type='TIMESTAMP'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_24', type='INTEGER'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_25', type='FLOAT'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_26', type='STRING'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_27', type='BOOLEAN'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_28', type='DATE'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_29', type='TIMESTAMP'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_30', type='INTEGER'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_31', type='FLOAT'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_32', type='STRING'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_33', type='BOOLEAN'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_34', type='DATE'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_35', type='TIMESTAMP'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_36', type='INTEGER'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_37', type='FLOAT'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_38', type='STRING'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_39', type='BOOLEAN'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_40', type='DATE'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_41', type='TIMESTAMP'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_42', type='INTEGER'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_43', type='FLOAT'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_44', type='STRING'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_45', type='BOOLEAN'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_46', type='DATE'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_47', type='TIMESTAMP'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_48', type='INTEGER'))
table_schema.fields.append(bigquery.TableFieldSchema(name='col_49', type='FLOAT'))

schema = 'col_0:INTEGER,col_1:FLOAT,col_2:STRING,col_3:BOOLEAN,col_4:DATE,col_5:TIMESTAMP,col_6:INTEGER,col_7:FLOAT,col_8:STRING,col_9:BOOLEAN,col_10:DATE,col_11:TIMESTAMP,col_12:INTEGER,col_13:FLOAT,col_14:STRING,col_15:BOOLEAN,col_16:DATE,col_17:TIMESTAMP,col_18:INTEGER,col_19:FLOAT,col_20:STRING,col_21:BOOLEAN,col_22:DATE,col_23:TIMESTAMP,col_24:INTEGER,col_25:FLOAT,col_26:STRING,col_27:BOOLEAN,col_28:DATE,col_29:TIMESTAMP,col_30:INTEGER,col_31:FLOAT,col_32:STRING,col_33:BOOLEAN,col_34:DATE,col_35:TIMESTAMP,col_36:INTEGER,col_37:FLOAT,col_38:STRING,col_39:BOOLEAN,col_40:DATE,col_41:TIMESTAMP,col_42:INTEGER,col_43:FLOAT,col_44:STRING,col_45:BOOLEAN,col_46:DATE,col_47:TIMESTAMP,col_48:INTEGER,col_49:FLOAT'

table_spec = bigquery.TableReference(
    projectId='viraj-patil-bootcamp',
    datasetId='dfbatch',
    tableId='t100k')

table = (rows | 'ToBigQuery' >> beam.io.WriteToBigQuery(
    table_spec,
    schema=schema,
    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))

result = pipeline.run()
result.wait_until_finish()
