# Nice Sync

Синхронизация, архивирование и удалённый доступ к файлам в сети личных устройств.

- [Цель и сценарии](docs/vision.md)
- [Этапы и открытые идеи](docs/roadmap.md)
- [Руководство по языку network.sync](docs/language.md)
- [Полный пример network.sync](examples/network.sync)
- [Отчёты по шагам и навигация по коду](docs/development-log.md)
- [Правила работы и роли агентов](AGENTS.md)

## Установка

Установите Rust с Cargo. Запустите эти команды из корня проекта.

```sh
cargo install --path . --locked
nice-sync --help
```

Для локальной сборки используйте `cargo build --locked --release`. Исполняемый файл появится в `target/release` под именем `nice-sync.exe` на Windows или `nice-sync` на Linux. В PowerShell можно запустить его как `.\target\release\nice-sync.exe --help`.

## Первое подключение

Сохраните файл `network.sync` на хосте и ноутбуке. Этот пример связывает две папки с документами. Пути отсчитываются от каталога конфига на соответствующем устройстве.

```text
network home {
    device server
    device laptop

    share docs {
        host server at "server-docs"
        mirror laptop at "laptop-docs" on delete propagate on conflict keep-both
    }

    invitation connect-laptop {
        issued by server
        grant browse, download, mirror on docs
        expires in 15m
        uses 1
    }
}
```

На хосте проверьте конфигурацию, примените её и запустите сервер.

```sh
nice-sync --state .nice-sync/server check network.sync
nice-sync --state .nice-sync/server plan network.sync --device server
nice-sync --state .nice-sync/server apply network.sync --device server
nice-sync --state .nice-sync/server serve
```

В другом терминале хоста получите код. Укажите `--qr`, если удобнее отсканировать тот же код с экрана.

```sh
nice-sync --state .nice-sync/server invite --template connect-laptop --device laptop
```

На ноутбуке запустите `join`, вставьте код и нажмите Enter. Хост автоматически зарегистрирует ключ ноутбука и выдаст права из приглашения. Сразу после входа можно просмотреть публикации командой `ls`.

```sh
nice-sync --state .nice-sync/laptop join
nice-sync --state .nice-sync/laptop ls
```

Для автоматической синхронизации создайте `laptop-docs` рядом с конфигом и примените правила.

```sh
nice-sync --state .nice-sync/laptop apply network.sync --device laptop
nice-sync --state .nice-sync/laptop sync --share docs
```

Положите файл в любой из двух каталогов и повторите `sync`. Для обмена в фоне в открытом терминале используйте `nice-sync --state .nice-sync/laptop sync --every 10`. На одном компьютере можно повторить пример в разных терминалах и запустить хост командой `nice-sync --state .nice-sync/server serve --direct 127.0.0.1:47631`.

По умолчанию Iroh ищет доступный прямой маршрут и может использовать релей. Если у хоста уже есть адрес в вашей локальной сети или VPN, передайте его и UDP-порт в `serve --direct`. В приглашение попадёт этот адрес. Для прямого режима используйте стабильный адрес и порт, доступные со второго устройства. Программа работает с текущими маршрутами и настройками ОС.

## Локальный архив

```sh
nice-sync --state .nice-sync/archive archive index camera
nice-sync --state .nice-sync/archive archive history photo.jpg
nice-sync --state .nice-sync/archive archive get photo.jpg restored.jpg --version 1
```

Укажите каталог состояния вне исходной папки. Каждая команда `index` сохраняет изменившиеся файлы, а сохранённые версии доступны после удаления исходника.

## Проверка

```sh
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo build --locked --release
```

Сетевые интеграционные тесты запускают хост и клиенты через UDP на `127.0.0.1`. На Linux их можно запустить в контейнере с образом Rust и смонтированным проектом или в Ubuntu под WSL.
