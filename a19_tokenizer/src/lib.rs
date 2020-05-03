
/// Represents a token string
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SearchToken<'a> {
    Value(&'a str),
    StartsWith(&'a str),
}

enum SearchTokenizerState {
    StartWith,
    Char,
    Hunting,
}

#[derive(PartialEq, Debug, Clone)]
enum SearchGroupState {
    None,
    Grouping,
}

pub fn search_tokenizer<'a>(value: &'a str) -> Vec<SearchToken<'a>> {
    let mut start_index = 0;
    let mut tokens = Vec::with_capacity(10);
    let mut current_state = SearchTokenizerState::Hunting;
    let mut group_state = SearchGroupState::None;
    for (p, c) in value.chars().enumerate() {
        match current_state {
            SearchTokenizerState::StartWith => {
                if !c.is_alphanumeric() {
                    match group_state {
                        SearchGroupState::Grouping => {
                            if c == '"' {
                                tokens.push(SearchToken::StartsWith(&value[start_index..p-1]));
                                current_state = SearchTokenizerState::Hunting;
                                group_state = SearchGroupState::None;
                            } else {
                                // Continue since not the end
                            }
                        }
                        _ => {
                            tokens.push(SearchToken::StartsWith(&value[start_index..p-1]));
                            current_state = SearchTokenizerState::Hunting;
                        }
                    }
                }
            }
            SearchTokenizerState::Hunting => {
                if c == '"' {
                    group_state = SearchGroupState::Grouping;
                } else if c.is_alphanumeric() {
                    start_index = p;
                    current_state = SearchTokenizerState::Char;
                }
            }
            SearchTokenizerState::Char => {
                if c == '*' {
                    current_state = SearchTokenizerState::StartWith;
                } else if !c.is_alphanumeric() {
                    match group_state {
                        SearchGroupState::Grouping => {
                            if c == '"' {
                                group_state = SearchGroupState::None;
                                tokens.push(SearchToken::Value(&value[start_index..p]));
                            } else {
                                // Keep going
                            }
                        }
                        SearchGroupState::None => {
                            tokens.push(SearchToken::Value(&value[start_index..p]));
                            current_state = SearchTokenizerState::Hunting;
                        }
                    }
                }
            }
        }
    }
    match current_state {
        SearchTokenizerState::StartWith => {
            tokens.push(SearchToken::StartsWith(&value[start_index..value.len() - 1]));
        }
        SearchTokenizerState::Char => {
            tokens.push(SearchToken::Value(&value[start_index..value.len()]));
        }
        _ => {}
    }
    tokens
}

#[cfg(test)]
mod tests {

    use super::*;
    
    #[test]
    pub fn token_tests() {
        let tokens = search_tokenizer("Hello World!");
        assert_eq!(vec![SearchToken::Value("Hello"), SearchToken::Value("World")], tokens);
    }

    #[test]
    pub fn token_tests_no_end() {
        let tokens = search_tokenizer("Hello World");
        assert_eq!(vec![SearchToken::Value("Hello"), SearchToken::Value("World")], tokens);
    }
    
    #[test]
    pub fn token_tests_no_numeric_start_no_end() {
        let tokens = search_tokenizer("*Hello World");
        assert_eq!(vec![SearchToken::Value("Hello"), SearchToken::Value("World")], tokens);
    }

    #[test]
    pub fn token_tests_no_numeric_starts_with() {
        let tokens = search_tokenizer("Hello* World*");
        assert_eq!(vec![SearchToken::StartsWith("Hello"), SearchToken::StartsWith("World")], tokens);
    }

    #[test]
    pub fn token_tests_no_numeric_starts_with_nums() {
        let tokens = search_tokenizer("Hello12* World*");
        assert_eq!(vec![SearchToken::StartsWith("Hello12"), SearchToken::StartsWith("World")], tokens);
    }

    #[test]
    pub fn group_test_whole() {
        let tokens = search_tokenizer("\"Hello World*");
        assert_eq!(vec![SearchToken::StartsWith("Hello World")], tokens);
    }
    
    #[test]
    pub fn group_test_whole_mix() {
        let tokens = search_tokenizer("\"Hello World*\" Something");
        assert_eq!(vec![SearchToken::StartsWith("Hello World"), SearchToken::Value("Something")], tokens);
    }

    #[test]
    pub fn group_test_whole_mix_starts() {
        let tokens = search_tokenizer("\"Hello World*\" Something*");
        assert_eq!(vec![SearchToken::StartsWith("Hello World"), SearchToken::StartsWith("Something")], tokens);
    }
}
