from squirrels import profile_manager as pm


def test_profile_manager():
    profile_name = 'unit-test'
    profile1 = pm.Profile(profile_name)
    profile2 = pm.Profile(profile_name)
    profile1.delete()
    assert(profile_name not in pm.get_profiles())

    inputs = [
        {'dialect': 'sqlite', 'conn_url': 'url1', 'username': 'user1', 'password': 'pass1'},
        {'dialect': 'mysql', 'conn_url': 'url2', 'username': 'user2', 'password': 'pass200'}
    ]

    expecteds = [
        {'dialect': 'sqlite', 'conn_url': 'url1', 'username': 'user1', 'password': '*****'},
        {'dialect': 'mysql', 'conn_url': 'url2', 'username': 'user2', 'password': '*******'}
    ]
    
    for inp, expected in zip(inputs, expecteds):
        profile1.set(**inp)
        assert(profile1.get() == expected)
        assert(profile2.get() == expected)
        assert(profile_name in pm.get_profiles())

    profile2.delete()
    assert(profile_name not in pm.get_profiles())
