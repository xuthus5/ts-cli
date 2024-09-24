package internal

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/olekukonko/tablewriter"
	"github.com/openGemini/go-prompt"
	"golang.org/x/term"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strings"
)

type Command string

const (
	Rp        Command = "rp"
	Set       Command = "set"
	Use       Command = "use"
	Help      Command = "help"
	Show      Command = "show"
	Drop      Command = "drop"
	Kill      Command = "kill"
	Auth      Command = "auth"
	Grant     Command = "grant"
	Alter     Command = "alter"
	Revoke    Command = "revoke"
	Insert    Command = "insert"
	Create    Command = "create"
	Explain   Command = "explain"
	Precision Command = "precision"
)

type CommandLine struct {
	Username        string
	Password        string
	Precision       string
	Database        string
	RetentionPolicy string
	prompt          *prompt.Prompt
}

func (cl *CommandLine) Run() {
	defer cl.deconstruct(nil)
	if cl.prompt == nil {
		completer := NewCompleter()
		cl.prompt = prompt.New(
			cl.executor,
			completer.completer,
			prompt.OptionTitle("openGemini: interactive openGemini client"),
			prompt.OptionPrefix("> "),
			prompt.OptionPrefixTextColor(prompt.DefaultColor),
			prompt.OptionCompletionWordSeparator(string([]byte{' ', os.PathSeparator})),
			prompt.OptionAddASCIICodeBind(
				prompt.ASCIICodeBind{
					ASCIICode: []byte{0x1b, 0x62},
					Fn:        prompt.GoLeftWord,
				},
				prompt.ASCIICodeBind{
					ASCIICode: []byte{0x1b, 0x66},
					Fn:        prompt.GoRightWord,
				},
			),
			prompt.OptionAddKeyBind(
				prompt.KeyBind{
					Key: prompt.ShiftLeft,
					Fn:  prompt.GoLeftWord,
				},
				prompt.KeyBind{
					Key: prompt.ShiftRight,
					Fn:  prompt.GoRightWord,
				},
				prompt.KeyBind{
					Key: prompt.ControlC,
					Fn:  cl.deconstruct,
				},
			),
		)
	}
	cl.prompt.Run()
}

func (cl *CommandLine) executor(s string) {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return
	}
	if s == "exit" || s == "quit" || s == "\\q" {
		cl.deconstruct(nil)
	}
	var err error
	switch cl.command(s) {
	case Help:
		err = cl.help()
	case Use:
		err = cl.use(s)
	case Rp:
		err = cl.retentionPolicy(s)
	case Auth:
		err = cl.auth()
	case Precision:
		err = cl.precision(s)
	case Insert:
		err = cl.write(s)
	case Show, Drop, Create, Explain, Kill, Grant, Alter, Revoke, Set:
		err = cl.query(s)
	default:
		err = errors.New(string("unsupported command: " + cl.command(s)))
	}
	if err != nil {
		fmt.Println(err)
	}
}

func (cl *CommandLine) auth() error {
	fmt.Printf("username: ")
	_, _ = fmt.Scanf("%s\n", &cl.Username)
	fmt.Printf("password: ")
	password, _ := term.ReadPassword(int(os.Stdin.Fd()))
	cl.Password = string(password)
	fmt.Printf("\n")
	return nil
}

func (cl *CommandLine) use(arg string) error {
	parts := strings.Fields(arg)
	if len(parts) <= 1 {
		return errors.New("invalid argument, use [db name]")
	}
	cl.Database = parts[1]
	return nil
}

func (cl *CommandLine) retentionPolicy(arg string) error {
	parts := strings.Fields(arg)
	if len(parts) <= 1 {
		return errors.New("invalid argument, rp [retention policy]")
	}
	cl.RetentionPolicy = parts[1]
	return nil
}

func (cl *CommandLine) precision(arg string) error {
	parts := strings.Fields(arg)
	if len(parts) <= 1 {
		return errors.New("invalid argument, precision [rfc3339,h,m,s,ms,u,ns]")
	}
	cl.Precision = parts[1]
	return nil
}

func (cl *CommandLine) help() error {
	fmt.Println(
		`Usage:
	auth                    prompts for username and password
	use <db name>           sets current database
	precision <format>      specifies the format of the timestamp: rfc3339, h, m, s, ms, u or ns
	exit/quit/ctrl+d        quits the openGemini shell

	show databases          show database names
	show series             show series information
	show measurements       show measurement information
	show tag keys           show tag key information
	show field keys         show field key information

	A full list of openGemini commands can be found at:
	https://docs.opengemini.org`)
	return nil
}

func (cl *CommandLine) write(arg string) error {
	return stdHttpClient.write(&WriteValue{
		Database:        cl.Database,
		RetentionPolicy: cl.RetentionPolicy,
		LineProtocol:    strings.NewReader(arg[7:]),
	})
}

func (cl *CommandLine) query(arg string) error {
	b, err := stdHttpClient.query(&QueryValue{
		Database:        cl.Database,
		RetentionPolicy: cl.RetentionPolicy,
		Command:         arg,
		Precision:       cl.Precision,
	})
	if err != nil {
		return err
	}
	cl.pretty(b)
	return nil
}

func (cl *CommandLine) command(arg string) Command {
	parts := strings.Fields(arg)
	return Command(parts[0])
}

func (cl *CommandLine) deconstruct(_ *prompt.Buffer) {
	if runtime.GOOS != "windows" {
		reset := exec.Command("stty", "-raw", "echo")
		reset.Stdin = os.Stdin
		_ = reset.Run()
	}
	os.Exit(0)
}

func (cl *CommandLine) pretty(r []byte) {
	var qr = new(QueryResult)
	err := json.Unmarshal(r, qr)
	if err != nil {
		fmt.Println(err)
		return
	}

	var w = os.Stdout
	for _, result := range qr.Results {
		for _, series := range result.Series {
			var tags []string
			for k, v := range series.Tags {
				tags = append(tags, fmt.Sprintf("%s=%s", k, v))
				sort.Strings(tags)
			}

			if series.Name != "" {
				_, _ = fmt.Fprintf(w, "name: %s\n", series.Name)
			}
			if len(tags) != 0 {
				_, _ = fmt.Fprintf(w, "tags: %s\n", strings.Join(tags, ", "))
			}

			writer := tablewriter.NewWriter(w)
			cl.prettyTable(series, writer)
			writer.Render()
			caption := fmt.Sprintf("%d columns, %d rows in set", len(series.Columns), len(series.Values))
			fmt.Println(caption)
			fmt.Println("")
		}
	}

}

func (cl *CommandLine) prettyTable(series *Series, w *tablewriter.Table) {
	w.SetAutoFormatHeaders(false)
	w.SetHeader(series.Columns)
	for _, value := range series.Values {
		tuple := make([]string, len(value))
		for i, val := range value {
			tuple[i] = fmt.Sprintf("%v", val)
		}
		w.Append(tuple)
	}
}

func NewCommandLine() *CommandLine {
	return &CommandLine{}
}

type Completer struct {
}

func NewCompleter() *Completer {
	return &Completer{}
}

func (c *Completer) completer(d prompt.Document) []prompt.Suggest {
	return []prompt.Suggest{}
}

// SeriesResult contains the results of a series query
type SeriesResult struct {
	Series []*Series `json:"series,omitempty"`
	Error  string    `json:"error,omitempty"`
}

// QueryResult is the top-level struct
type QueryResult struct {
	Results []*SeriesResult `json:"results,omitempty"`
	Error   string          `json:"error,omitempty"`
}

type SeriesValue []interface{}

type SeriesValues []SeriesValue

// Series defines the structure for series data
type Series struct {
	Name    string            `json:"name,omitempty"`
	Tags    map[string]string `json:"tags,omitempty"`
	Columns []string          `json:"columns,omitempty"`
	Values  SeriesValues      `json:"values,omitempty"`
}
