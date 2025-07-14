from processamento_dados import Dados

path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'
path_dados_combinados = 'data_processed/dados_combinados.csv'


#Extração
print("Iniciando o processamento")
dados_empresaA = Dados.leitura_dados(path_json,'json')
dados_empresaB = Dados.leitura_dados(path_csv,'csv')

print("Dados carregados em memória")

#Transform 
key_mapping = {
    'Nome do Item': 'Nome do Produto',
    'Classificação do Produto': 'Categoria do Produto',
    'Valor em Reais (R$)': 'Preço do Produto (R$)',
    'Quantidade em Estoque': 'Quantidade em Estoque',
    'Nome da Loja': 'Filial',
    'Data da Venda': 'Data da Venda'
}

dados_empresaB.rename_columns(key_mapping)
print("Padronização das colunas")

print(f'Arquivo da empresa A possui {dados_empresaA.qtd_linhas} linhas')
print(f'Arquivo da empresa B possui {dados_empresaB.qtd_linhas} linhas')

print('Iniciando a unificação')
dados_fusao = Dados.join(dados_empresaA, dados_empresaB)

print(f'O arquivo gerado possui {dados_fusao.qtd_linhas} linhas')

#Load 
print('Preparando para salvar os dados.')
dados_fusao.salvando_dados(path_dados_combinados)
print(f'Processo concluído com sucesso, arquivo disponível em: {path_dados_combinados}')

