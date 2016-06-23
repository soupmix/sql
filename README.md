# Soupmix


[![Build Status](https://travis-ci.org/soupmix/sql.svg?branch=master)](https://travis-ci.org/soupmix/sql) [![Latest Stable Version](https://poser.pugx.org/soupmix/sql/v/stable)](https://packagist.org/packages/soupmix/sql) [![Total Downloads](https://poser.pugx.org/soupmix/sql/downloads)](https://packagist.org/packages/soupmix/sql) [![Latest Unstable Version](https://poser.pugx.org/soupmix/sql/v/unstable)](https://packagist.org/packages/soupmix/sql) [![License](https://poser.pugx.org/soupmix/sql/license)](https://packagist.org/packages/soupmix/sql)
[![Scrutinizer Code Quality](https://scrutinizer-ci.com/g/soupmix/sql/badges/quality-score.png?b=master)](https://scrutinizer-ci.com/g/soupmix/sql/)

Simple low level SQL adapter to handle CRUD operations written in PHP and built on top of Doctrine/DBAL. This library does not provide any ORM or ODM. 


## Installation

It's recommended that you use [Composer](https://getcomposer.org/) to install Soupmix.

```bash
$ composer require soupmix/sql "~0.5"
```

This will install Soupmix and all required dependencies. Soupmix requires PHP 5.6.0 or newer.

## Documentation

[API Documentation](https://github.com/soupmix/base/blob/master/docs/API_Documentation.md): See details about the db adapters functions:

## Usage
```
// Connect to SQL Service
$config = [
    'db_name'   => 'test',
    'user_name' => 'user',
    'password'  => '',
    'host'      => '127.0.0.1',
    'port'      => 3306,
    'charset'   => 'utf8',
    'driver'    => 'pdo_mysql',
]
$sql=new Soupmix\SQL($adapter_config);


$docs = [];
$docs[] = [
    "full_name" => "John Doe",
    "age" => 33,
    "email"    => "johndoe@domain.com"      
];
$docs[] = [
    "full_name" => "Jack Doe",
    "age" => 38,
    "email"    => "jackdoe@domain.com"
];

$docs[] = [
    "full_name" => "Jane Doe",
    "age" => 29,
    "email"    => "janedoe@domain.com"
];

foreach($docs as $doc){
    // insert user into database
    $sql_user_id = $sql->insert("users",$doc);
}
// get user data using id
$user_data = $sql->get('users', $sql_user_id);



// user's age lower_than_and_equal to 34 or greater_than_and_equal 36 but not 38
$filter = [[['age__lte'=>34],['age__gte'=>36]],"age__not"=>38];

//find users that has criteria encoded in $filter
$docs = $sql->find("users", $filter);


```



## Contribute
* Open issue if found bugs or sent pull request.
* Feel free to ask if you have any questions.
