﻿using System.Collections.Generic;

namespace WorkloadExecutorDynamoDB
{
    public class Workload
    {
        public decimal Read { get; set; }

        public decimal Update { get; set; }

        public decimal Insert { get; set; }

        public List<string> Ids { get; set; }
    }
}
