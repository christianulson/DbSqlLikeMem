using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Windows;
using System.Threading.Tasks;

namespace DbSqlLikeMem.VisualStudioExtension.XamlHarness;

/// <summary>
/// EN: Hosts the XAML harness window used to preview the extension outside Visual Studio.
/// PT: Hospeda a janela do harness XAML usada para visualizar a extensao fora do Visual Studio.
/// </summary>
public partial class MainWindow : Window, INotifyPropertyChanged
{
    private readonly DbSqlLikeMemToolWindowControl toolWindowControl;
    private bool isInitializing;
    private bool harnessEnvironmentLoaded;

    /// <summary>
    /// EN: Gets whether the harness window is still loading the initial environment.
    /// PT: Indica se a janela do harness ainda está carregando o ambiente inicial.
    /// </summary>
    public bool IsInitializing
    {
        get => isInitializing;
        private set
        {
            if (isInitializing == value)
            {
                return;
            }

            isInitializing = value;
            OnPropertyChanged();
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public event PropertyChangedEventHandler? PropertyChanged;

    /// <summary>
    /// EN: Initializes the harness window and hooks the environment load flow.
    /// PT: Inicializa a janela do harness e conecta o fluxo de carregamento do ambiente.
    /// </summary>
    public MainWindow()
    {
        InitializeComponent();
        toolWindowControl = new DbSqlLikeMemToolWindowControl();
        PreviewHost.Content = toolWindowControl;
        Loaded += OnLoaded;
    }

    private void OnLoaded(object sender, RoutedEventArgs e)
        => _ = OnLoadedAsync();

    private async Task OnLoadedAsync()
    {
        if (harnessEnvironmentLoaded)
        {
            return;
        }

        harnessEnvironmentLoaded = true;
        IsInitializing = true;

        try
        {
            await Task.Yield();

            var app = (App)Application.Current;
            var loadTask = app.HarnessEnvironmentLoadTask;
            if (loadTask is null)
            {
                return;
            }

            var connections = await loadTask.ConfigureAwait(true);
            await toolWindowControl.LoadHarnessConnectionsAsync(connections).ConfigureAwait(true);
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            MessageBox.Show(this, ex.Message, "DbSqlLikeMem XAML harness", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            IsInitializing = false;
        }
    }

    private void OnPropertyChanged([CallerMemberName] string? propertyName = null)
        => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
}
