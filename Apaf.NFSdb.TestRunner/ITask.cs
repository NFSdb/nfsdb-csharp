namespace Apaf.NFSdb.TestRunner
{
    public interface ITask
    {
        void Run();
        string Name { get; }
    }
}