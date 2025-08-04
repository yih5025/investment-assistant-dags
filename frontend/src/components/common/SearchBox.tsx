import React, { useState, useRef, useEffect } from 'react';
import { Search, X } from 'lucide-react';
import { cn } from '../../utils/helpers';
import { debounce } from '../../utils/helpers';

interface SearchBoxProps {
  placeholder?: string;
  value?: string;
  onChange?: (value: string) => void;
  onSearch?: (value: string) => void;
  onClear?: () => void;
  disabled?: boolean;
  autoFocus?: boolean;
  debounceMs?: number;
  className?: string;
  suggestions?: string[];
  showSuggestions?: boolean;
  onSuggestionSelect?: (suggestion: string) => void;
}

export const SearchBox: React.FC<SearchBoxProps> = ({
  placeholder = '검색...',
  value = '',
  onChange,
  onSearch,
  onClear,
  disabled = false,
  autoFocus = false,
  debounceMs = 300,
  className,
  suggestions = [],
  showSuggestions = false,
  onSuggestionSelect,
}) => {
  const [inputValue, setInputValue] = useState(value);
  const [showSuggestionList, setShowSuggestionList] = useState(false);
  const [selectedSuggestionIndex, setSelectedSuggestionIndex] = useState(-1);
  const inputRef = useRef<HTMLInputElement>(null);
  const suggestionsRef = useRef<HTMLUListElement>(null);

  // 디바운스된 onChange 핸들러
  const debouncedOnChange = debounce((newValue: string) => {
    if (onChange) {
      onChange(newValue);
    }
  }, debounceMs);

  useEffect(() => {
    setInputValue(value);
  }, [value]);

  useEffect(() => {
    if (autoFocus && inputRef.current) {
      inputRef.current.focus();
    }
  }, [autoFocus]);

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const newValue = e.target.value;
    setInputValue(newValue);
    setSelectedSuggestionIndex(-1);
    
    if (showSuggestions && newValue.trim()) {
      setShowSuggestionList(true);
    } else {
      setShowSuggestionList(false);
    }

    debouncedOnChange(newValue);
  };

  const handleInputKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (!showSuggestionList || suggestions.length === 0) {
      if (e.key === 'Enter' && onSearch) {
        onSearch(inputValue);
      }
      return;
    }

    switch (e.key) {
      case 'ArrowDown':
        e.preventDefault();
        setSelectedSuggestionIndex(prev => 
          prev < suggestions.length - 1 ? prev + 1 : 0
        );
        break;
      case 'ArrowUp':
        e.preventDefault();
        setSelectedSuggestionIndex(prev => 
          prev > 0 ? prev - 1 : suggestions.length - 1
        );
        break;
      case 'Enter':
        e.preventDefault();
        if (selectedSuggestionIndex >= 0) {
          handleSuggestionSelect(suggestions[selectedSuggestionIndex]);
        } else if (onSearch) {
          onSearch(inputValue);
        }
        break;
      case 'Escape':
        setShowSuggestionList(false);
        setSelectedSuggestionIndex(-1);
        inputRef.current?.blur();
        break;
    }
  };

  const handleSuggestionSelect = (suggestion: string) => {
    setInputValue(suggestion);
    setShowSuggestionList(false);
    setSelectedSuggestionIndex(-1);
    
    if (onSuggestionSelect) {
      onSuggestionSelect(suggestion);
    }
    if (onChange) {
      onChange(suggestion);
    }
  };

  const handleClear = () => {
    setInputValue('');
    setShowSuggestionList(false);
    setSelectedSuggestionIndex(-1);
    
    if (onClear) {
      onClear();
    }
    if (onChange) {
      onChange('');
    }
    
    inputRef.current?.focus();
  };

  const handleSearchClick = () => {
    if (onSearch) {
      onSearch(inputValue);
    }
  };

  const filteredSuggestions = suggestions.filter(suggestion =>
    suggestion.toLowerCase().includes(inputValue.toLowerCase())
  );

  return (
    <div className={cn('relative', className)}>
      <div className="relative">
        <Search 
          className="absolute left-3 top-1/2 transform -translate-y-1/2 text-muted-foreground h-4 w-4" 
        />
        <input
          ref={inputRef}
          type="text"
          value={inputValue}
          onChange={handleInputChange}
          onKeyDown={handleInputKeyDown}
          onFocus={() => {
            if (showSuggestions && inputValue.trim() && filteredSuggestions.length > 0) {
              setShowSuggestionList(true);
            }
          }}
          onBlur={() => {
            // 짧은 지연을 두어 클릭 이벤트가 처리될 수 있도록 함
            setTimeout(() => setShowSuggestionList(false), 150);
          }}
          placeholder={placeholder}
          disabled={disabled}
          className={cn(
            'w-full pl-10 pr-10 py-2 border border-input rounded-md',
            'bg-background text-foreground',
            'placeholder:text-muted-foreground',
            'focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2',
            'disabled:cursor-not-allowed disabled:opacity-50',
          )}
        />
        <div className="absolute right-2 top-1/2 transform -translate-y-1/2 flex items-center space-x-1">
          {inputValue && (
            <button
              onClick={handleClear}
              disabled={disabled}
              className="p-1 hover:bg-muted rounded"
              type="button"
            >
              <X className="h-4 w-4 text-muted-foreground" />
            </button>
          )}
          {onSearch && (
            <button
              onClick={handleSearchClick}
              disabled={disabled || !inputValue.trim()}
              className="p-1 hover:bg-muted rounded disabled:opacity-50"
              type="button"
            >
              <Search className="h-4 w-4 text-muted-foreground" />
            </button>
          )}
        </div>
      </div>

      {showSuggestionList && filteredSuggestions.length > 0 && (
        <ul
          ref={suggestionsRef}
          className={cn(
            'absolute z-50 w-full mt-1 bg-background border border-input rounded-md shadow-lg',
            'max-h-60 overflow-auto'
          )}
        >
          {filteredSuggestions.map((suggestion, index) => (
            <li
              key={suggestion}
              className={cn(
                'px-3 py-2 cursor-pointer hover:bg-muted',
                'first:rounded-t-md last:rounded-b-md',
                index === selectedSuggestionIndex && 'bg-muted'
              )}
              onClick={() => handleSuggestionSelect(suggestion)}
            >
              {suggestion}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
};