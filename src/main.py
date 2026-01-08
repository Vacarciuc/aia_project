from src.command import Command, CommandEnum, RequestParams

def main() -> None:

    request_params = RequestParams()
    request_params.latitude = 47.0269
    request_params.longitude = 28.8416
    request_params.start_date = "2020-01-01"
    request_params.end_date = "2026-01-01"

    command = Command()
    command.execute(CommandEnum.API_REQUEST, request_params)
    print("Command API request executed with success!")
    command.execute(CommandEnum.SAVE_CLEAN_DATA, request_params)
    print("Command clean data executed with success!")
    command.execute(CommandEnum.ANALYZE_DATA, request_params)
    print("Command analyze data executed with success!")

    print('Done!')


if __name__ == '__main__':
    main()


