﻿namespace WorkloadGenerator
{
    public class Configuration
    {
        public int Count { get; set; }
        public double Read { get; set; }
        public double Update { get; set; }
        public double Insert { get; set; }

        public int ComplexQuery { get; set; }
    }
}
