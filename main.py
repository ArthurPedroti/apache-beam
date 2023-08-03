import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)

colunas_dengue = [
  'id',
  'data_iniSE',
  'casos',
  'ibge_code',
  'cidade',
  'uf',
  'cep',
  'latitude',
  'longitude'
]

def lista_para_dicionario(elemento, colunas):
  return dict(zip(colunas, elemento))

def texto_para_lista (elemento, delimitador='|'):
  """
  Recebe um texto e um delimitador
  Retorna uma lista de elementors pelo delimitador
  """
  return elemento.split(delimitador)

def trata_datas(elemento):
  """
  Recebe um dicionario e cria um novo campo com ANO-MES
  Retorna o mesmo dicionario com o mesmo campo
  """
  elemento['ano_mes'] = '-'.join(elemento['data_iniSE'].split('-')[:2])
  return elemento

def chave_uf(elemento):
  """
  Receber um dicionario
  Retornar uma tupla com o estado(UF) e o elemento (UF, dicionario)
  """
  
  chave = elemento['uf']
  return (chave, elemento)

def casos_dengue(elemento):
  """
  Recebe uma tupla ('RS', [{}, {}])
  return um tupla ('RS-2014-12'), 8.0)
  """
  uf, registros = elemento
  for registro in registros:
    if bool(re.search(r'\d', registro['casos'])):
      yield (f"{uf}-{registro['ano_mes']}", float(registro['casos']))
    else:
      yield (f"{uf}-{registro['ano_mes']}", 0.0)

def chave_uf_ano_mes_de_lista(elemento):
  """
    Receber uma lista de elementos
    Retornar uma tupla contendo uma chave e o valor de chuva em mm
    ('UF-ANO-MES, 1.3)
  """
  data, mm, uf = elemento
  chave = '-'.join([uf, *data.split('-')[:2]])
  if float(mm) < 0:
    mm = 0.0
  else:
    mm = float(mm)
    
  return (chave, mm)

def filtra_campos_vazios(elemento):
  """
    Remove elementos que tenham chaves vazias
    Recebe uma tupla: ('CE-2015-01', {'chuvas': [85.8], 'dengue': [175.0]})
    Retorna uma tupla: ('CE-2015-01', {'chuvas': [85.8], 'dengue': [175.0]})
  """
  chave, dados = elemento
  if all([
      dados['chuvas'],
      dados['dengue']
    ]):
    return True
  return False

def descompactar_elementos(elem):
  """
    Receber uma tupla ('CE-2015-01', {'chuvas': [85.8], 'dengue': [175.0]})
    Retornar uma tupla ('CE',2015,01, 85.8, 175.0)
  """
  chave, dados = elem
  chuva = dados['chuvas'][0]
  dengue = dados['dengue'][0]
  uf, ano, mes = chave.split('-')
  return uf, ano, mes, str(chuva), str(dengue)

dengue = (
  pipeline
  | "Leitura dos dados de dengue" >> 
    ReadFromText('casos_dengue.txt', skip_header_lines=1)
    # ReadFromText('sample_casos_dengue.txt', skip_header_lines=1)
  | "De texto para lista" >> beam.Map(lambda x: x.split('|'))
  # | "De texto para lista" >> beam.Map(texto_para_lista)
  | "De lita para dicionario" >> beam.Map(lista_para_dicionario, colunas_dengue)
  | "Criar campo ano_mes" >> beam.Map(trata_datas)
  | "Criar chave pelo estado" >> beam.Map(chave_uf)
  | "Agrupar pelo estado" >> beam.GroupByKey()
  | "Descompactar casos de dengue" >> beam.FlatMap(casos_dengue)
  | "Soma dos casos pela chave" >> beam.CombinePerKey(sum)
  # | "Mostrar resultados" >> beam.Map(print)
)

chuvas = (
  pipeline 
  | "Leitura dos dados de chuvas" >> 
    ReadFromText('chuvas.csv', skip_header_lines=1)
    # ReadFromText('sample_chuvas.csv', skip_header_lines=1)
  | "De texto para lista (chuvas)" >> beam.Map(texto_para_lista, delimitador=',')
  | "Criando a chave UF-ANO-MES" >> beam.Map(chave_uf_ano_mes_de_lista)
  | "Soma do total de chuvas pela chave" >> beam.CombinePerKey(sum)
  | "Arredondar resultados de chuvas" >> beam.Map(lambda x: (x[0], round(x[1], 1)))
  # | "Mostrar resultados (chuvas)" >> beam.Map(print)
)

resultado = (
  # (chuvas, dengue)
  # | "Empilha as pcols" >> beam.Flatten()
  # | "Agrupa" >> beam.GroupByKey()
  ({'chuvas': chuvas, 'dengue': dengue})
  | "Mesclar pcols" >> beam.CoGroupByKey()
  | "Filtrar dados vazios" >> beam.Filter(filtra_campos_vazios)
  | "Descompactar elementos" >> beam.Map(descompactar_elementos)
  | "Preparar csv" >> beam.Map(lambda x: ";".join(x))
  # | "Mostrar resultados (uniao)" >> beam.Map(print)
)

header = 'UF;ANO;MES;CHUVA;DENGUE'

resultado | 'Criar arquivo CSV' >> WriteToText('resultado', file_name_suffix='.csv', header=header)
 
pipeline.run()