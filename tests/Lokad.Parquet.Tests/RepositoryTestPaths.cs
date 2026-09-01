namespace Lokad.Parquet.Tests;

internal static class RepositoryTestPaths
{
    static RepositoryTestPaths()
    {
        for (var directory = new DirectoryInfo(AppContext.BaseDirectory); directory is not null; directory = directory.Parent)
        {
            if (File.Exists(Path.Combine(directory.FullName, "Lokad.Parquet.slnx")))
            {
                Root = directory.FullName;
                return;
            }
        }
        throw new InvalidOperationException("The repository root could not be located.");
    }

    public static string Root { get; }
}
