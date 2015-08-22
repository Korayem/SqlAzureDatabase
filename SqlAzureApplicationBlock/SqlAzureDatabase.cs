using System;
using System.Globalization;
using System.Text;
using Microsoft.Practices.EnterpriseLibrary.Data.Sql;
using System.Data.Common;
using System.Data.SqlClient;
using Microsoft.Practices.EnterpriseLibrary.Data;
using System.Data;
using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;

namespace SqlAzureApplicationBlock
{
   public class SqlAzureDatabase:SqlDatabase
   {
       private readonly RetryPolicy _retryPolicy;
       private readonly DbProviderFactory _dbProviderFactory;
       private readonly string _federationName;
       private readonly string _distributionName;
       private readonly FederationType _federationType;
       private readonly object _federationKey;
       

       public SqlAzureDatabase(string connectionString)
           : this(connectionString, RetryPolicyFactory.GetDefaultSqlConnectionRetryPolicy(),FederationType.None, null, null, null)
       {
       }

       public SqlAzureDatabase(string connectionString, FederationType federationType, string federationName, string distributionName, object federationKey)
           : this(connectionString, RetryPolicyFactory.GetDefaultSqlConnectionRetryPolicy(), federationType, federationName, distributionName, federationKey)
       {

       }

       public SqlAzureDatabase(string connectionString, RetryPolicy retryPolicy, FederationType federationType, string federationName, string distributionName, object federationKey)
           : base(connectionString)
       {
           _retryPolicy = retryPolicy;
           _dbProviderFactory = SqlClientFactory.Instance;
           _federationType = federationType;
           _federationName = federationName;
           _distributionName = distributionName;
           _federationKey = federationKey;
              

       }

       //public FederationType Federation { get; set; }

       //public string FederationName { get; set; }

       //public string DistributionName { get; set; }

       //public object FederationKey { get; set; }

       public DbConnection OpenConnection()
       {
           DbConnection connection = this.GetNewOpenConnection();
           this.ExecuteFederationCommand(connection);
           return connection;
       }
       
       public override int ExecuteNonQuery(DbCommand command)
       {
           using (DatabaseConnectionWrapper wrapper = GetOpenConnection())
           {
               if (this._federationType != FederationType.All)
               {
                   ExecuteFederationCommand(wrapper.Connection);
                   PrepareCommand(command, wrapper.Connection);
                   return DoExecuteNonQueryWithRetry(command);
               }
               else
               {
                   PrepareCommand(command, wrapper.Connection);
                   return ExecuteNonQueryFanout(wrapper, command);
               }
           }
       }

       private int ExecuteNonQueryFanout(DatabaseConnectionWrapper wrapper, DbCommand command)
       {
           long? federationKey = 0;
           while (federationKey != null)
           {
               ExecuteFederationCommand(wrapper.Connection,FederationType.Member,federationKey,false);
               DoExecuteNonQueryWithRetry(command);
               using (DbCommand fCommand = _dbProviderFactory.CreateCommand())
               {
                   fCommand.CommandText = "SELECT CAST(range_high as bigint) FROM sys.federation_member_distributions";
                   PrepareCommand(fCommand,wrapper.Connection);
                   object key = DoExecuteScalarWithRetry(fCommand);
                   if (key != DBNull.Value)
                   {
                       federationKey = Convert.ToInt64(key);
                   }
                   else
                   {
                       federationKey = null;
                   }
               }

           }
           return 0;
       }


       public override IDataReader ExecuteReader(DbCommand command, DbTransaction transaction)
       {
           PrepareCommand(command, transaction);
           return DoExecuteReaderWithRetry(command, CommandBehavior.Default);
           
       }
       public override IDataReader ExecuteReader(DbCommand command)
        {
            using(DatabaseConnectionWrapper wrapper = GetOpenConnection())
            {
                ExecuteFederationCommand(wrapper.Connection);
                PrepareCommand(command, wrapper.Connection);
                IDataReader realReader = DoExecuteReaderWithRetry(command, CommandBehavior.Default);
                return CreateWrappedReader(wrapper, realReader);
            }
        }

        public override object ExecuteScalar(DbCommand command)
        {
            if (command == null) throw new ArgumentNullException("command");

            using (var wrapper = GetOpenConnection())
            {
                ExecuteFederationCommand(wrapper.Connection);
                PrepareCommand(command, wrapper.Connection);
                return DoExecuteScalarWithRetry(command);
            }
        }

        

        private int DoExecuteNonQueryWithRetry(DbCommand command)
        {
            if (command == null) throw new ArgumentNullException("command");

            SqlCommand sqlCommand = command as SqlCommand;
            if(sqlCommand != null)
            {
                DateTime startTime = DateTime.Now;
                int rowsAffected = sqlCommand.ExecuteNonQueryWithRetry(_retryPolicy);
                // instrumentationProvider.FireCommandExecutedEvent(startTime);
                return rowsAffected;
            }
            return 0;
        }

        private IDataReader DoExecuteReaderWithRetry(DbCommand command, CommandBehavior cmdBehavior)
        {
            SqlCommand sqlCommand = command as SqlCommand;
            if(sqlCommand != null)
            {
                DateTime startTime = DateTime.Now;
                IDataReader reader = sqlCommand.ExecuteReaderWithRetry(_retryPolicy);
                //     instrumentationProvider.FireCommandExecutedEvent(startTime);
                return reader;
            }
            return null;
        }

        private object DoExecuteScalarWithRetry(IDbCommand command)
        {
            SqlCommand sqlCommand = command as SqlCommand;
            if(sqlCommand != null)
            {
                DateTime startTime = DateTime.Now;
                object returnValue = sqlCommand.ExecuteScalarWithRetry(_retryPolicy);
                //  instrumentationProvider.FireCommandExecutedEvent(startTime);
                return returnValue;
            }
            return null;
        }

        private DbConnection GetNewOpenConnection()
        {
            SqlConnection connection = null;
            try
            {
                connection = CreateConnection() as SqlConnection;
                if(connection != null)
                {
                    connection.OpenWithRetry(this._retryPolicy);
                }

                //instrumentationProvider.FireConnectionOpenedEvent();
            }
            catch
            {
                if (connection != null)
                    connection.Close();

                throw;
            }

            return connection;
        }

        protected override DatabaseConnectionWrapper GetWrappedConnection()
        {
            return new DatabaseConnectionWrapper(GetNewOpenConnection());
        }

        private void ExecuteFederationCommand(DbConnection connection, FederationType federationType, object federationKey, bool filterOn)
       {
           DbCommand federationCommand = this.GetFederationCommand(federationType, federationKey, filterOn);
           PrepareCommand(federationCommand, connection);
           DoExecuteNonQueryWithRetry(federationCommand);
       }
        private void ExecuteFederationCommand(DbConnection connection)
        {
            if (this._federationType != FederationType.None)
            {
                ExecuteFederationCommand(connection, this._federationType, this._federationKey, true);
            }
        }

        private DbCommand GetFederationCommand(FederationType federationType, object federationKey, bool filterOn)
        {
            DbCommand command = _dbProviderFactory.CreateCommand();
            command.CommandText = this.GetUseFederationStatement(federationType, federationKey, filterOn);
            return command;

        }

        private string GetUseFederationStatement(FederationType federationType, object federationKey, bool filterOn)
        {
            return federationType == FederationType.Root
                       ? "USE FEDERATION ROOT WITH RESET"
                       : GetMemberFederationStatement(filterOn, federationKey);
        }

        private string GetMemberFederationStatement(bool filterOn, object federationKey)
        {
            return _federationType == FederationType.Root
                       ? "USE FEDERATION ROOT WITH RESET"
                       : string.Format("USE FEDERATION {0} ({1}='{2}') WITH RESET, FILTERING = {3}", _federationName,
                                       _distributionName, federationKey, (filterOn ? "ON" : "OFF"));
        }

   }
}
