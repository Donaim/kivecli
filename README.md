
# Kive CLI

Collection of scripts to interact with the [Kive platform](https://github.com/cfe-lab/Kive) from the comfort of your terminal.

## Environment variables

`kivecli` relies on a few variables for authentication.

```bash
export MICALL_KIVE_SERVER=https://kive.example.com
export MICALL_KIVE_USER=myuser
export MICALL_KIVE_PASSWORD=secret
```

## Example usage

Run a simple pipeline:

```bash
kivecli run my_app input_dataset
```

# License

This program is licensed under GNU GENERAL PUBLIC LICENSE version 3.
More details in the [COPYING](./COPYING) file.
