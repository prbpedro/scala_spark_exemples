Para rodar o programa é necessário rodar os seguintes comandos:
1 - start-all.sh (Para iniciar todos os serviços do hadoop)
2 - startNetworkServer & (Para inciar a base de dados Derby para o metastore do Hive)
3 - hive --service metastore & (Para iniciar o metastore do hive)
4 - hive --service hiveserver2 & (Para iniciar o hive)



