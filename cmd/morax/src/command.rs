// Copyright 2024 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::path::PathBuf;

use clap::Parser;
use clap::Subcommand;
use error_stack::ResultExt;
use morax_version::version;

use crate::config::Config;
use crate::Error;

#[derive(Debug, Parser)]
#[command(name = "morax", version, long_version = version())]
pub struct Command {
    #[command(subcommand)]
    pub cmd: SubCommand,
}

impl Command {
    pub fn run(self) -> error_stack::Result<(), Error> {
        match self.cmd {
            SubCommand::Start(cmd) => cmd.run(),
            SubCommand::Generate(cmd) => cmd.run(),
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum SubCommand {
    /// Start a Morax broker node.
    #[command()]
    Start(CommandStart),
    /// Generate command-line interface utilities.
    #[command(name = "gen")]
    Generate(CommandGenerate),
}

#[derive(Debug, Parser)]
pub struct CommandStart {
    /// Configure the server with the given file; if not specified, the
    /// [default configuration][crate::config::Config::default] is used.
    #[arg(short, long)]
    config_file: Option<PathBuf>,
}

impl CommandStart {
    pub fn run(self) -> error_stack::Result<(), Error> {
        let config = if let Some(file) = self.config_file {
            let content = std::fs::read_to_string(&file).change_context_lazy(|| {
                Error(format!("failed to read config file: {}", file.display()))
            })?;
            toml::from_str(&content)
                .change_context_lazy(|| Error("failed to parse config content".to_string()))?
        } else {
            Config::default()
        };

        morax_runtime::init(&config.runtime);
        ctrlc::set_handler(move || {
            morax_runtime::shutdown();
        })
        .change_context_lazy(|| Error("failed to setup ctrl-c signal handle".to_string()))?;

        let rt = morax_runtime::make_runtime("morax-main", "morax-main", 1);
        rt.block_on(async move {
            morax_telemetry::init(&config.telemetry);
            let state = morax_server::start(config.server)
                .await
                .change_context_lazy(|| {
                    Error("A fatal error has occurred in Morax server process.".to_string())
                })?;
            state.await_shutdown().await;
            Ok(())
        })
    }
}

#[derive(Debug, Parser)]
pub struct CommandGenerate {
    #[arg(short, long, global = true)]
    output: Option<PathBuf>,

    #[command(subcommand)]
    cmd: GenerateTarget,
}

#[derive(Debug, Subcommand)]
pub enum GenerateTarget {
    /// Generate the default server config.
    #[command()]
    SampleConfig,
}

impl CommandGenerate {
    pub fn run(self) -> error_stack::Result<(), Error> {
        match self.cmd {
            GenerateTarget::SampleConfig => {
                let config = Config::default();
                let content = toml::to_string(&config).change_context_lazy(|| {
                    Error("default config must be always valid".to_string())
                })?;
                if let Some(output) = self.output {
                    std::fs::write(&output, content).change_context_lazy(|| {
                        Error(format!("failed to write config to {}", output.display()))
                    })?;
                } else {
                    println!("{content}");
                }
            }
        }

        Ok(())
    }
}
