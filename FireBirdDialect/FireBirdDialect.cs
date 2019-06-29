using Dapper;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace FireBirdDialect
{
    /// <summary>
    /// Classe do dialeto do Firebird.
    /// </summary>
    public class FireBirdDialect
    {
        private readonly IDbConnection _connection;

        /// <summary>
        /// Construtor da classe.
        /// </summary>
        /// <param name="connection">Contexto a ser utilizado.</param>
        public FireBirdDialect(IDbConnection connection)
        {
            _connection = connection;
        }

        /// <summary>
        /// Método de seleção.
        /// </summary>
        /// <param name="table">Tabela na qual os dados serão selecionados.</param>
        /// <returns>SQL de seleção a ser executada.</returns>
        public string Select(string table)
        {
            return $"SELECT * FROM {table}";
        }

        /// <summary>
        /// Método de seleção com clausula where.
        /// </summary>
        /// <param name="table">Tabela na qual os dados serão selecionados.</param>
        /// <returns>SQL de seleção a ser executada com a clausula where formada pelas chaves primárias.</returns>
        public string SelectWithWhere(string table)
        {
            var chaves = GetKeys(table);
            var sql = new StringBuilder();
            sql.AppendLine($"SELECT * FROM {table}");
            sql.AppendLine("WHERE");


            for (var i = 0; i < chaves.Count(); i++)
            {
                sql.Append($"{chaves.ToArray()[i]} = @{chaves.ToArray()[i]}");
                if (i + 1 != chaves.Count())
                    sql.AppendLine(" AND ");
            }

            return sql.ToString();
        }

        /// <summary>
        /// Método de exclusão.
        /// </summary>
        /// <param name="table">Tabela na qual os dados serão excluídos.</param>
        /// <returns>SQL de exclusão a ser executada com a clausula where formada pelas chaves primárias.</returns>
        public string Delete(string table)
        {
            var chaves = GetKeys(table);

            var sWhere = new StringBuilder();
            sWhere.AppendLine("WHERE");

            for (var i = 0; i < chaves.Count(); i++)
            {
                sWhere.Append($"{chaves.ToArray()[i]} = @{chaves.ToArray()[i]}");
                if (i + 1 != chaves.Count())
                    sWhere.AppendLine(" AND ");
            }

            return string.Format("DELETE FROM {0} {1}", table, sWhere);
        }

        /// <summary>
        /// Método de inserção.
        /// </summary>
        /// <param name="table">Tabela na qual os dados serão inseridos.</param>
        /// <returns>SQL de inserção a ser executada.</returns>
        public string InsertInto(string table)
        {
            var sql = new StringBuilder();
            var campos = GetCampos(table);

            sql.AppendLine($"INSERT INTO {table}");
            sql.Append("(");
            for (var i = 0; i < campos.Count(); i++)
            {
                sql.Append(campos.ToArray()[i]);
                if (i + 1 != campos.Count())
                    sql.Append(",");
            }
            sql.AppendLine(")");
            sql.AppendLine(" VALUES ");
            sql.Append("(");
            for (var i = 0; i < campos.Count(); i++)
            {
                sql.Append($"@{campos.ToArray()[i]}");
                if (i + 1 != campos.Count())
                    sql.Append(",");
            }

            sql.AppendLine(")");

            return sql.ToString();
        }

        /// <summary>
        /// Método para atualização.
        /// </summary>
        /// <param name="table">Tabela na qual os dados serão atualizados.</param>
        /// <returns>SQL de atualização com a clausula where formada pelas chaves primárias.</returns>
        public string Update(string table)
        {
            var sql = new StringBuilder();
            var campos = GetCampos(table);
            var chaves = GetKeys(table);

            sql.AppendLine("UPDATE " + table);
            sql.Append("SET ");

            for (var i = 0; i < campos.Count(); i++)
            {
                sql.Append(string.Format("{0} = @{1}", campos.ToArray()[i], campos.ToArray()[i]));
                if (i + 1 != campos.Count())
                    sql.AppendLine(",");
            }
            sql.AppendLine();
            sql.AppendLine("WHERE");

            for (var i = 0; i < chaves.Count(); i++)
            {
                sql.Append(string.Format("{0} = @{1}", chaves.ToArray()[i], chaves.ToArray()[i]));
                if (i + 1 != chaves.Count())
                    sql.AppendLine(" AND ");
            }
            return sql.ToString();

        }

        private IEnumerable<string> GetCampos(string tabela)
        {
            var campos = new List<string>();

            if (DataCache.FieldsTable.Where(p => p.Table.ToLower() == tabela.ToLower()).Count() > 0)
            {
                foreach (var item in DataCache.FieldsTable.Where(p => p.Table.ToLower() == tabela.ToLower()))
                    campos.Add(item.Field);
                return campos;
            }

            var sql = new StringBuilder();

            sql.Append("select RF.RDB$FIELD_NAME as Campo,FF.RDB$FIELD_LENGTH as Tamanho   ,");
            sql.Append("CASE FF.RDB$FIELD_TYPE  ");
            sql.Append("WHEN 261 THEN 'BLOB' ");
            sql.Append("WHEN 14 THEN 'CHAR' ");
            sql.Append("WHEN 40 THEN 'CSTRING' ");
            sql.Append("WHEN 11 THEN 'D_FLOAT' ");
            sql.Append("WHEN 27 THEN 'DOUBLE' ");
            sql.Append("WHEN 10 THEN 'FLOAT' ");
            sql.Append("WHEN 16 THEN 'INT64' ");
            sql.Append("WHEN 8 THEN 'INTEGER' ");
            sql.Append("WHEN 9 THEN 'QUAD' ");
            sql.Append("WHEN 7 THEN 'SMALLINT' ");
            sql.Append("WHEN 12 THEN 'DATE' ");
            sql.Append("WHEN 13 THEN 'TIME' ");
            sql.Append("WHEN 35 THEN 'TIMESTAMP' ");
            sql.Append("WHEN 37 THEN 'VARCHAR' ");
            sql.Append("ELSE 'UNKNOWN' ");
            sql.Append("END AS Tipo, ");
            sql.Append("RF.RDB$NULL_FLAG ");
            sql.Append("from RDB$RELATION_FIELDS RF ");
            sql.Append("LEFT JOIN RDB$FIELDS FF ON RF.RDB$FIELD_SOURCE = FF.RDB$FIELD_NAME ");
            sql.Append("WHERE RF.RDB$SYSTEM_FLAG=0 ");
            sql.Append("and RF.RDB$RELATION_NAME = '" + tabela + "' ");
            sql.Append("ORDER BY ");
            sql.Append("RF.RDB$FIELD_POSITION");

            var rs = _connection.Query<dynamic>(sql.ToString()).ToList();

            foreach (var item in rs)
            {
                campos.Add(item.CAMPO.Trim());
                DataCache.FieldsTable.Add(new ItemCache()
                {
                    Field = item.CAMPO.Trim(),
                    Table = tabela.ToLower()
                });
            }

            return campos;
        }

        private IEnumerable<string> GetKeys(string tabela)
        {
            var campos = new List<string>();

            if (DataCache.FieldsKeys.Where(p => p.Table.ToLower() == tabela.ToLower()).Count() > 0)
            {
                foreach (var item in DataCache.FieldsKeys.Where(p => p.Table.ToLower() == tabela.ToLower()))
                    campos.Add(item.Field);
                return campos;
            }

            var sql = new StringBuilder();

            sql.Append("select ");
            sql.Append("ix.rdb$index_name as index_name, ");
            sql.Append("    sg.rdb$field_name as field_name, ");
            sql.Append("    rc.rdb$relation_name as table_name ");
            sql.Append("from ");
            sql.Append("    rdb$indices ix ");
            sql.Append("    left join rdb$index_segments sg on ix.rdb$index_name = sg.rdb$index_name ");
            sql.Append("    left join rdb$relation_constraints rc on rc.rdb$index_name = ix.rdb$index_name ");
            sql.Append("where ");
            sql.Append("    rc.rdb$constraint_type = 'PRIMARY KEY' ");
            sql.Append("    and rc.rdb$relation_name = '" + tabela + "' ");

            var rs = _connection.Query<dynamic>(sql.ToString()).ToList();

            foreach (var item in rs)
            {
                campos.Add(item.FIELD_NAME.Trim());
                DataCache.FieldsKeys.Add(new ItemCache()
                {
                    Field = item.FIELD_NAME.Trim(),
                    Table = tabela.ToLower()
                });
            }

            return campos;
        }

        /// <summary>
        /// Gera o proximo sequencial
        /// Ex.: GEN_ID(GEN_ID_PAGAMENTO,1)
        /// </summary>
        /// <typeparam name="T">Tipo do retorno, normalmente é Int</typeparam>
        /// <param name="generator">Nome do generator</param>
        /// <param name="incremento">Passo</param>
        /// <returns>Id gerado</returns>
        public T GenID<T>(string generator, int incremento = 1)
        {
            var sql = new StringBuilder();
            sql.AppendFormat("select GEN_ID({0},{1}) FROM RDB$DATABASE", generator, incremento);
            return _connection.Query<T>(sql.ToString()).FirstOrDefault();
        }
    }
}
