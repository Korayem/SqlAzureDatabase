using System;
using System.Collections.Generic;
using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;

namespace SqlAzureApplicationBlock
{
    public class RetryPolicyFactory
    {

        static RetryPolicyFactory()
        {
            var strategy = new FixedInterval("fixed", 10, TimeSpan.FromSeconds(3));
            var strategies = new List<RetryStrategy> { strategy };
            var manager = new RetryManager(strategies, "fixed");
            RetryManager.SetDefault(manager);
            
        }
        public static RetryPolicy GetDefaultSqlConnectionRetryPolicy()
        {
            return RetryManager.Instance.GetDefaultSqlConnectionRetryPolicy();
            
        }
    }
}