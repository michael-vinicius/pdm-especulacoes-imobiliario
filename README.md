# Processamento_de_dados_massivos--Dados-imobiliarios-

> Este repositório foi criado como parte da atividade final do curso de Processamento de Dados Massivos. O projeto tem como objetivo aplicar técnicas avançadas de engenharia de dados e inteligência artificial no domínio imobiliário, explorando oportunidades de análise e predição baseadas em grandes volumes de dados.


# Pipelines de dados.

---
Ajuste os nomes dos arquivos conforme a sua necessidade!
### carrega a Bronze:
```bash

python Medallion/bronze_dataframe.py `
  --input "C:\Users\marco\OneDrive\Documentos\GitHub\ML-data-service\dataframes\backup\Goiania.csv" `
  --outdir "C:\Users\marco\OneDrive\Documentos\GitHub\ML-data-service\dataframes\bronze"
```

###  carrega a Silver:
```bash
python Medallion/silver_dataframe.py `
  --bronze "C:\Users\marco\OneDrive\Documentos\GitHub\ML-data-service\dataframes\bronze\listings_bronze_20251109T200306Z.parquet" `
  --outdir "C:\Users\marco\OneDrive\Documentos\GitHub\ML-data-service\dataframes\silver"
```

###  carrega a Gold:
```bash
python Medallion/gold_dataframe.py `
  --bronze "C:\Users\marco\OneDrive\Documentos\GitHub\ML-data-service\dataframe\bronze\listings_bronze_*.parquet" `
  --outdir "C:\Users\marco\OneDrive\Documentos\GitHub\ML-data-service\dataframe\silver"
```