using System.Collections.Generic;

namespace WorkloadGenerator
{
    public class Workload
    {
        public double Read { get; set; }

        public double Update { get; set; }

        public double Insert { get; set; }

        public int ComplexQuery { get; set; }

        public List<string> Ids { get; set; }

        public int Thread { get; set; }
    }
}
