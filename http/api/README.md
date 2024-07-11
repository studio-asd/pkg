# API

API package is a set of framework of standard for building HTTP APIs. The package is made with APIs documentation in-mind that allows the package to produce an API documentation like:

- https://stripe.com/docs/api
- https://docs.github.com/en/rest
- https://docs.crunchybridge.com/api

This document does not cover on how the available information in the API group and each API level transformed into the documentation itself. Separate tools will be used to transform retrieve and transform the informations into a readable and well-structured documentation.

The goal of this package is to:

- Make it possible to retrieve the API group and API informations from the APIs package.
- Create a human-readable API documentation that intuitive and easy to read.
- Have an always up-to-date auto-generated API documentation.
- Experimenting to create an auto-generated fuzzy testing for API request validation.

This pacakge is insprided by [Brandur's](https://brandur.org/) `nanoglyphs` called: [Docs! Docs! Docs!](https://brandur.org/nanoglyphs/031-api-docs).

## Internationalization

The package supports i18n response message by providing a special message called
I18nMessage. Currently only support EN(English) and IDN(Indonesia).

## Request Type

1. JSON Body

2. Path Parameters

3. Query String

4. Post Form Body

## Response Type

### JSON

The JSON response is standarized to follow the format below.

**OK(200)** response:

```json
{
  "message": "some message",
  "data": {
      "key": "value"
  }
}
```

1. Message

    Message can be used to communicate the result/intent of the APIs to the end user.

2. Data

    Data is the result of API request, it can be something that need to be shown to the end user.

---

**ERROR(Non-200)** response:

```json
{
  "message": "error message",
  "data": {
    "key": "value"
  },
  "error": {
    "code": "error_code",
    "message": "error message",
    "retry": {
      "retryable": true,
      "max_retry": 3
    },
    "errors": [
      {
        "message": "error message 1"
      }
      {
        "message": "error message 2"
      }
    ]
  }
}
```

1. Message

2. 

## Documentation First

Documentation is hard, and we might need to do other things than making our
documentation up-to-date. This happens to many companies and documentation as
they grow and this is a pain-point to any API gateways. As we know, swagger
sucks and OpenAPI standard might not be the one that we need because there's a
lot of things that we are not using and we need to learn the standard all over
again.

API documentation is useful for both internal and external customers. And it is
crucial for development process as it is needed by multiple teams at the company
where the internal customers might be:

- Backend Engineers

  Usually, the `Backend Engineers` are the one who exports and implements the
  APIs logic, and sometimes they need to look at the collection of the APIs to
  do some tasks. For example, doing an integration or end to end tests.

- Frontend Engineers

  The `Frontend Engineers` need to consume the APIs that created by the
  `Backend Engineers` and they need to know the intention of the API and how to
  read them. If the internal frontend team is having a hard time reading the API
  documentation, then the customers must be having a harder time to read it.

- Security Team

  The `Security Team` at a company might want to review the API before they are
  going public to minize the possibility of security issue and abuse to the API.
  The security team need to understand the API in-order to do that, and
  analyzing other people works might takes more time. If the documentation is
  not helpful then the team will have a hard time to analyze the API.

- Product Team

  The `Product Team` usually wants to share the `public` API to external parties
  for integration purpose. And when they do, they usually asks the software
  engineering team about the available API and its detail. It would be
  beneficial for both parties(product team and external engineering team) if the
  documentation is human-readable.
