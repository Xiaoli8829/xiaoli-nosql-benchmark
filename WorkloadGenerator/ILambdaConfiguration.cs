using Microsoft.Extensions.Configuration;

namespace WorkloadGenerator
{
    public interface ILambdaConfiguration
    {
        IConfiguration Configuration { get; }
    }
}
