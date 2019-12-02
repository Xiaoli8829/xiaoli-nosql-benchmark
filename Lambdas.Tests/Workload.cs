using System.Collections.Generic;

namespace Lambdas.Tests
{
    public class Workload
    {
        public decimal Read { get; set; }

        public decimal Update { get; set; }

        public decimal Insert { get; set; }

        public int ComplexQuery { get; set; }

        public List<string> Ids { get; set; }
    }
}
