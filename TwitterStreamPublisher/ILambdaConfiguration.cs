using Microsoft.Extensions.Configuration;

namespace TwitterStreamPublisher
{
    public interface ILambdaConfiguration
    {
        IConfiguration Configuration { get; }
    }
}
