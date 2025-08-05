import { useState } from "react";
import { ExternalLink, User, Clock, TrendingUp } from "lucide-react";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Avatar, AvatarFallback, AvatarImage } from "@/components/ui/avatar";
import { Badge } from "@/components/ui/badge";

interface SocialPost {
  id: string;
  author: string;
  handle: string;
  avatar: string;
  content: string;
  timestamp: string;
  platform: 'twitter' | 'truth';
  url: string;
  impact: 'high' | 'medium' | 'low';
  tags: string[];
}

const SocialFeed = () => {
  const [expandedPost, setExpandedPost] = useState<string | null>(null);

  // Mock social media posts
  const posts: SocialPost[] = [
    {
      id: '1',
      author: 'Elon Musk',
      handle: '@elonmusk',
      avatar: '/api/placeholder/40/40',
      content: 'Tesla Q4 production numbers are looking incredible! Best quarter ever. Manufacturing is hard, but we are getting better at it every day. 🚗⚡',
      timestamp: '2시간 전',
      platform: 'twitter',
      url: 'https://twitter.com/elonmusk/status/123',
      impact: 'high',
      tags: ['Tesla', 'TSLA', '생산']
    },
    {
      id: '2',
      author: 'Donald Trump',
      handle: '@realDonaldTrump',
      avatar: '/api/placeholder/40/40',
      content: 'The economy is doing GREAT! Stock market at record highs. America First policies are working. Big tech companies should bring jobs back to USA!',
      timestamp: '4시간 전',
      platform: 'truth',
      url: 'https://truthsocial.com/@realdonaldtrump/123',
      impact: 'high',
      tags: ['경제', '주식', '정책']
    },
    {
      id: '3',
      author: 'Elon Musk',
      handle: '@elonmusk',
      avatar: '/api/placeholder/40/40',
      content: 'Dogecoin to the moon! 🐕🚀 Just kidding... or am I? 😉',
      timestamp: '6시간 전',
      platform: 'twitter',
      url: 'https://twitter.com/elonmusk/status/124',
      impact: 'medium',
      tags: ['Dogecoin', 'DOGE', '암호화폐']
    }
  ];

  const getPlatformIcon = (platform: string) => {
    switch (platform) {
      case 'twitter':
        return <span className="text-blue-500">𝕏</span>;
      case 'truth':
        return <span className="text-red-600">T</span>;
      default:
        return <User className="h-4 w-4" />;
    }
  };

  const getImpactColor = (impact: string) => {
    switch (impact) {
      case 'high':
        return 'border-l-destructive bg-destructive/5';
      case 'medium':
        return 'border-l-warning bg-warning/5';
      case 'low':
        return 'border-l-muted bg-muted/5';
      default:
        return 'border-l-muted';
    }
  };

  const getImpactBadge = (impact: string) => {
    switch (impact) {
      case 'high':
        return <Badge variant="destructive" className="text-xs">High Impact</Badge>;
      case 'medium':
        return <Badge variant="secondary" className="text-xs">Medium Impact</Badge>;
      case 'low':
        return <Badge variant="outline" className="text-xs">Low Impact</Badge>;
      default:
        return null;
    }
  };

  const toggleExpanded = (postId: string) => {
    setExpandedPost(expandedPost === postId ? null : postId);
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center space-x-2">
        <TrendingUp className="h-5 w-5 text-primary" />
        <h2 className="text-lg font-semibold">인플루언서 피드</h2>
        <Badge variant="secondary" className="text-xs">실시간</Badge>
      </div>

      <div className="space-y-3">
        {posts.map((post) => (
          <Card 
            key={post.id}
            className={`p-4 border-l-4 transition-all hover:shadow-md ${getImpactColor(post.impact)}`}
          >
            <div className="space-y-3">
              {/* Header */}
              <div className="flex items-start justify-between">
                <div className="flex items-center space-x-3">
                  <Avatar className="h-10 w-10">
                    <AvatarImage src={post.avatar} alt={post.author} />
                    <AvatarFallback>{post.author.split(' ').map(n => n[0]).join('')}</AvatarFallback>
                  </Avatar>
                  <div>
                    <div className="flex items-center space-x-2">
                      <span className="font-semibold text-sm">{post.author}</span>
                      <span className="text-xs text-muted-foreground">{post.handle}</span>
                      {getPlatformIcon(post.platform)}
                    </div>
                    <div className="flex items-center space-x-2 mt-1">
                      <Clock className="h-3 w-3 text-muted-foreground" />
                      <span className="text-xs text-muted-foreground">{post.timestamp}</span>
                      {getImpactBadge(post.impact)}
                    </div>
                  </div>
                </div>
              </div>

              {/* Content */}
              <div className="space-y-2">
                <p className={`text-sm leading-relaxed ${
                  expandedPost === post.id ? '' : 'line-clamp-3'
                }`}>
                  {post.content}
                </p>
                
                {post.content.length > 100 && (
                  <Button 
                    variant="ghost" 
                    size="sm" 
                    className="h-auto p-0 text-xs text-primary"
                    onClick={() => toggleExpanded(post.id)}
                  >
                    {expandedPost === post.id ? '접기' : '더 보기'}
                  </Button>
                )}
              </div>

              {/* Tags */}
              <div className="flex flex-wrap gap-1">
                {post.tags.map((tag, index) => (
                  <Badge key={index} variant="outline" className="text-xs">
                    #{tag}
                  </Badge>
                ))}
              </div>

              {/* Actions */}
              <div className="flex items-center justify-between pt-2 border-t">
                <Button 
                  variant="ghost" 
                  size="sm"
                  onClick={() => window.open(post.url, '_blank')}
                  className="text-xs"
                >
                  <ExternalLink className="h-3 w-3 mr-1" />
                  원문 보기
                </Button>
                <span className="text-xs text-muted-foreground">
                  시장 영향도: {post.impact === 'high' ? '높음' : post.impact === 'medium' ? '보통' : '낮음'}
                </span>
              </div>
            </div>
          </Card>
        ))}
      </div>

      <Button variant="outline" className="w-full">
        더 많은 피드 보기
      </Button>
    </div>
  );
};

export default SocialFeed;