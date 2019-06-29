# FireBirdDialect
Biblioteca que gera dinamicamente sentenças SQL dentro do padrão do banco de dados FireBird. Para usar com Dapper e DapperExtensions

# Exemplo:

var connectionString = @"User=SYSDBA;Password=masterkey;Database=SEU_BANCO.FDB;DataSource=localhost;Port=3050;Dialect=3;Charset=NONE;Role=;Connection lifetime=15;Pooling=true;MinPoolSize=0;MaxPoolSize=50;Packet Size=8192;ServerType=0;";

var connection = new FbConnection(connectionString);
connection.Open();

var helper = new FireBirdDialect.FireBirdDialect(connection);
var query = helper.SelectWithWhere("CIDADES");

var dados = connection.Query(query, new { CIDADE = "Divinópolis", UF = "MG" });
