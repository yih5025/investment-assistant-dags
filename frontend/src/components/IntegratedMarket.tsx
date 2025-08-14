// //integrated-market.tsx
// import { useState } from "react";
// import { Search, Star } from "lucide-react";
// import { MarketList } from "./MarketList";
// import { SearchDetail } from "./SearchDetail";

// interface IntegratedMarketProps {
//   isLoggedIn: boolean;
//   onLoginPrompt: () => void;
// }

// export function IntegratedMarket({ isLoggedIn, onLoginPrompt }: IntegratedMarketProps) {
//   const [searchQuery, setSearchQuery] = useState("");
//   const [selectedStock, setSelectedStock] = useState<string | null>(null);
//   const [watchlist, setWatchlist] = useState<string[]>(isLoggedIn ? ["AAPL", "TSLA", "NVDA"] : []);

//   const handleSearch = (query: string) => {
//     if (query.trim()) {
//       setSearchQuery(query);
//       setSelectedStock(query.toUpperCase());
//     }
//   };

//   const handleStockSelect = (symbol: string) => {
//     setSelectedStock(symbol);
//   };

//   const handleBackToMarket = () => {
//     setSelectedStock(null);
//     setSearchQuery("");
//   };

//   const toggleWatchlist = (symbol: string) => {
//     if (!isLoggedIn) {
//       onLoginPrompt();
//       return;
//     }

//     setWatchlist(prev => 
//       prev.includes(symbol) 
//         ? prev.filter(s => s !== symbol)
//         : [...prev, symbol]
//     );
//   };

//   if (selectedStock) {
//     return (
//       <SearchDetail 
//         symbol={selectedStock} 
//         onBack={handleBackToMarket}
//         isLoggedIn={isLoggedIn}
//         onLoginPrompt={onLoginPrompt}
//         isInWatchlist={watchlist.includes(selectedStock)}
//         onToggleWatchlist={() => toggleWatchlist(selectedStock)}
//       />
//     );
//   }

//   return (
//     <div className="space-y-6">
//       {/* 검색 헤더 */}
//       <div className="glass-card rounded-2xl p-4">
//         <div className="glass rounded-xl p-3 flex items-center space-x-3">
//           <Search size={20} className="text-foreground/60" />
//           <input 
//             type="text" 
//             placeholder="주식, 코인, 기업명을 검색하세요..."
//             value={searchQuery}
//             onChange={(e) => setSearchQuery(e.target.value)}
//             onKeyPress={(e) => e.key === 'Enter' && handleSearch(searchQuery)}
//             className="flex-1 bg-transparent placeholder-foreground/50 outline-none"
//           />
//           <button 
//             onClick={() => handleSearch(searchQuery)}
//             className="px-4 py-1 glass text-primary rounded-lg text-sm hover:glass-strong transition-all"
//           >
//             검색
//           </button>
//         </div>
        
//         {/* 인기 검색어 */}
//         <div className="mt-4">
//           <h3 className="font-medium mb-3">🔥 인기 검색어</h3>
//           <div className="flex flex-wrap gap-2">
//             {["AAPL", "TSLA", "NVDA", "BTC", "ETH", "META", "GOOGL", "MSFT"].map((symbol) => (
//               <button
//                 key={symbol}
//                 onClick={() => handleSearch(symbol)}
//                 className="px-3 py-1.5 glass rounded-lg text-sm hover:glass-strong transition-all flex items-center space-x-1"
//               >
//                 <span>{symbol}</span>
//                 {isLoggedIn && watchlist.includes(symbol) && (
//                   <Star size={12} className="text-yellow-400 fill-current" />
//                 )}
//               </button>
//             ))}
//           </div>
//         </div>
//       </div>

//       {/* 관심 종목 (로그인한 사용자만) */}
//       {isLoggedIn && watchlist.length > 0 && (
//         <div className="glass-card rounded-2xl p-4">
//           <div className="flex items-center justify-between mb-3">
//             <h3 className="font-medium flex items-center">
//               <Star className="mr-2 text-yellow-400" size={18} />
//               내 관심 종목
//             </h3>
//             <span className="text-xs text-foreground/60">{watchlist.length}개</span>
//           </div>
//           <div className="grid grid-cols-3 gap-2">
//             {watchlist.map((symbol) => (
//               <button
//                 key={symbol}
//                 onClick={() => handleStockSelect(symbol)}
//                 className="glass rounded-lg p-2 text-center hover:glass-strong transition-all"
//               >
//                 <div className="text-sm font-medium">{symbol}</div>
//                 <div className="text-xs text-green-400">+2.4%</div>
//               </button>
//             ))}
//           </div>
//         </div>
//       )}

//       {/* 실시간 시장 데이터 */}
//       <MarketList 
//         onStockSelect={handleStockSelect} 
//         isLoggedIn={isLoggedIn}
//         watchlist={watchlist}
//         onToggleWatchlist={toggleWatchlist}
//       />
//     </div>
//   );
// }