import WeatherDataLoader as WDL


def main():
    path = "C:/Users/enigma/Desktop/AIA/data sets"

    loader = WDL.WeatherDataLoader(
        root_path=path,
        file_names=[
            '72529014768',
            '72414013866',
            '72417713734',
            '72421093814',
            '72219013874',
            '72635094860',
            '72250012919',
            '72317013723',
            '72532014842',
            '72764524012'
        ]
    )

    data = loader.load_files_from_all_folders()

    print(data)


if __name__ == '__main__':
    main()