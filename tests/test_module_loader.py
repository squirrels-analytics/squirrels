from squirrels import module_loader

def test_parse_module_repo_strings():
    repo_strings = [
        'first_repo=https://github.com/user/repo1.git@v1.0',
        'https://github.com/user/repo2.git@v2.0',
        'https://github.com/user/repo3@v3.0'
    ]
    
    repos = module_loader.parse_module_repo_strings(repo_strings)
    expected = [
        ('first_repo', 'https://github.com/user/repo1.git', 'v1.0'), 
        ('repo2', 'https://github.com/user/repo2.git', 'v2.0'),
        ('repo3', 'https://github.com/user/repo3', 'v3.0')
    ]

    assert(repos == expected)
