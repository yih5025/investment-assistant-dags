import Header from "@/components/Layout/Header";
import StockBanner from "@/components/Home/StockBanner";
import EventCalendar from "@/components/Home/EventCalendar";
import SocialFeed from "@/components/Home/SocialFeed";
import NewsList from "@/components/Home/NewsList";

const Index = () => {
  return (
    <div className="min-h-screen bg-background">
      <Header />
      
      <main className="container mx-auto px-4 py-6 space-y-8">
        {/* Hero Section */}
        <div className="text-center space-y-4">
          <h1 className="text-3xl md:text-4xl font-bold bg-gradient-to-r from-primary to-accent bg-clip-text text-transparent">
            Wise & Easy Investment
          </h1>
          <p className="text-lg text-muted-foreground max-w-2xl mx-auto">
            투자를 더 쉽고 현명하게. 실시간 정보와 직관적인 분석으로 여러분의 투자를 도와드립니다.
          </p>
        </div>

        {/* Stock Banner */}
        <section>
          <StockBanner />
        </section>

        {/* Main Content Grid */}
        <div className="grid lg:grid-cols-2 gap-8">
          {/* Left Column */}
          <div className="space-y-8">
            <section>
              <EventCalendar />
            </section>
            
            <section>
              <SocialFeed />
            </section>
          </div>

          {/* Right Column */}
          <div>
            <section>
              <NewsList />
            </section>
          </div>
        </div>
      </main>
    </div>
  );
};

export default Index;
